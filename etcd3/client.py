import collections
import functools
import inspect
import sys
import threading

import grpc
import grpc._channel

import six
from six.moves import queue

import etcd3.etcdrpc as etcdrpc
import etcd3.exceptions as exceptions
import etcd3.leases as leases
import etcd3.locks as locks
import etcd3.members
import etcd3.role
import etcd3.transactions as transactions
import etcd3.user
import etcd3.utils as utils
import etcd3.watch as watch

_EXCEPTIONS_BY_CODE = {
    grpc.StatusCode.INTERNAL: exceptions.InternalServerError,
    grpc.StatusCode.UNAVAILABLE: exceptions.ConnectionFailedError,
    grpc.StatusCode.DEADLINE_EXCEEDED: exceptions.ConnectionTimeoutError,
    grpc.StatusCode.FAILED_PRECONDITION: exceptions.PreconditionFailedError,
    grpc.StatusCode.PERMISSION_DENIED: exceptions.PermissionDeniedError,
}

# Allow overriding exceptions based on the details coming back from the grpc
# exception.
_EXCEPTIONS_OVERRIDE = {
    grpc.StatusCode.FAILED_PRECONDITION: {
        "etcdserver: user name not found": exceptions.EntityNotFoundError,
        "etcdserver: role name not found": exceptions.EntityNotFoundError,
    },
}

# Grant permissions
_Permissions = collections.namedtuple("_Permissions", ["r", "w", "rw"])
Perms = _Permissions(
    etcdrpc.Permission.READ,
    etcdrpc.Permission.WRITE,
    etcdrpc.Permission.READWRITE,
)
"""Permissions object instance necessary for granting permssions in roles.

    :attribute r: Read only permission
    :attribute w: Write only permission
    :attribute rw: Read/write permission

"""


def _translate_exception(exc):
    code = exc.code()
    details = exc.details()
    exception = _EXCEPTIONS_OVERRIDE.get(code)
    if exception and details in exception:
        exception = exception[details]
    else:
        exception = _EXCEPTIONS_BY_CODE.get(code)

    if exception is None:
        raise
    if six.PY2:
        raise exception(details)
    elif six.PY3:
        tb = sys.exc_info()[2]
        raise exception(details).with_traceback(tb) from None


def _handle_errors(f):
    if inspect.isgeneratorfunction(f):
        def handler(*args, **kwargs):
            try:
                for data in f(*args, **kwargs):
                    yield data
            except grpc.RpcError as exc:
                _translate_exception(exc)
    else:
        def handler(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except grpc.RpcError as exc:
                _translate_exception(exc)

    return functools.wraps(f)(handler)


class Transactions(object):
    def __init__(self):
        self.value = transactions.Value
        self.version = transactions.Version
        self.create = transactions.Create
        self.mod = transactions.Mod

        self.put = transactions.Put
        self.get = transactions.Get
        self.delete = transactions.Delete
        self.txn = transactions.Txn


class KVMetadata(object):
    def __init__(self, keyvalue, header):
        self.key = keyvalue.key
        self.create_revision = keyvalue.create_revision
        self.mod_revision = keyvalue.mod_revision
        self.version = keyvalue.version
        self.lease_id = keyvalue.lease
        self.response_header = header


class Status(object):
    def __init__(self, version, db_size, leader, raft_index, raft_term):
        self.version = version
        self.db_size = db_size
        self.leader = leader
        self.raft_index = raft_index
        self.raft_term = raft_term


class Alarm(object):
    def __init__(self, alarm_type, member_id):
        self.alarm_type = alarm_type
        self.member_id = member_id


class EtcdTokenCallCredentials(grpc.AuthMetadataPlugin):
    """Metadata wrapper for raw access token credentials."""

    def __init__(self, access_token):
        self._access_token = access_token

    def __call__(self, context, callback):
        metadata = (('token', self._access_token),)
        callback(metadata, None)


class Etcd3Client(object):
    @_handle_errors
    def __init__(self, host='localhost', port=2379,
                 ca_cert=None, cert_key=None, cert_cert=None, timeout=None,
                 user=None, password=None, grpc_options=None):

        self._url = '{host}:{port}'.format(host=host, port=port)
        self.metadata = None

        cert_params = [c is not None for c in (cert_cert, cert_key)]
        if ca_cert is not None:
            if all(cert_params):
                credentials = self._get_secure_creds(
                    ca_cert,
                    cert_key,
                    cert_cert
                )
                self.uses_secure_channel = True
                self.channel = grpc.secure_channel(self._url, credentials,
                                                   options=grpc_options)
            elif any(cert_params):
                # some of the cert parameters are set
                raise ValueError(
                    'to use a secure channel ca_cert is required by itself, '
                    'or cert_cert and cert_key must both be specified.')
            else:
                credentials = self._get_secure_creds(ca_cert, None, None)
                self.uses_secure_channel = True
                self.channel = grpc.secure_channel(self._url, credentials,
                                                   options=grpc_options)
        else:
            self.uses_secure_channel = False
            self.channel = grpc.insecure_channel(self._url,
                                                 options=grpc_options)

        self.timeout = timeout
        self.call_credentials = None

        cred_params = [c is not None for c in (user, password)]

        self.auth_stub = etcdrpc.AuthStub(self.channel)
        if all(cred_params):
            auth_request = etcdrpc.AuthenticateRequest(
                name=user,
                password=password
            )

            resp = self.auth_stub.Authenticate(auth_request, self.timeout)
            self.metadata = (('token', resp.token),)
            self.call_credentials = grpc.metadata_call_credentials(
                EtcdTokenCallCredentials(resp.token))

        elif any(cred_params):
            raise Exception(
                'if using authentication credentials both user and password '
                'must be specified.'
            )

        self.kvstub = etcdrpc.KVStub(self.channel)
        self.watcher = watch.Watcher(
            etcdrpc.WatchStub(self.channel),
            timeout=self.timeout,
            call_credentials=self.call_credentials,
            metadata=self.metadata
        )
        self.clusterstub = etcdrpc.ClusterStub(self.channel)
        self.leasestub = etcdrpc.LeaseStub(self.channel)
        self.maintenancestub = etcdrpc.MaintenanceStub(self.channel)
        self.transactions = Transactions()

    def close(self):
        """Call the GRPC channel close semantics."""
        if hasattr(self, 'channel'):
            self.channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _get_secure_creds(self, ca_cert, cert_key=None, cert_cert=None):
        cert_key_file = None
        cert_cert_file = None

        with open(ca_cert, 'rb') as f:
            ca_cert_file = f.read()

        if cert_key is not None:
            with open(cert_key, 'rb') as f:
                cert_key_file = f.read()

        if cert_cert is not None:
            with open(cert_cert, 'rb') as f:
                cert_cert_file = f.read()

        return grpc.ssl_channel_credentials(
            ca_cert_file,
            cert_key_file,
            cert_cert_file
        )

    def _build_get_range_request(self, key,
                                 range_end=None,
                                 limit=None,
                                 revision=None,
                                 sort_order=None,
                                 sort_target='key',
                                 serializable=False,
                                 keys_only=False,
                                 count_only=False,
                                 min_mod_revision=None,
                                 max_mod_revision=None,
                                 min_create_revision=None,
                                 max_create_revision=None):
        range_request = etcdrpc.RangeRequest()
        range_request.key = utils.to_bytes(key)
        range_request.keys_only = keys_only
        range_request.count_only = count_only
        range_request.serializable = serializable

        if range_end is not None:
            range_request.range_end = utils.to_bytes(range_end)
        if limit is not None:
            range_request.limit = limit
        if revision is not None:
            range_request.revision = revision
        if min_mod_revision is not None:
            range_request.min_mod_revision = min_mod_revision
        if max_mod_revision is not None:
            range_request.max_mod_revision = max_mod_revision
        if min_create_revision is not None:
            range_request.min_mod_revision = min_create_revision
        if max_create_revision is not None:
            range_request.min_mod_revision = max_create_revision

        sort_orders = {
            None: etcdrpc.RangeRequest.NONE,
            'ascend': etcdrpc.RangeRequest.ASCEND,
            'descend': etcdrpc.RangeRequest.DESCEND,
        }
        request_sort_order = sort_orders.get(sort_order)
        if request_sort_order is None:
            raise ValueError('unknown sort order: "{}"'.format(sort_order))
        range_request.sort_order = request_sort_order

        sort_targets = {
            None: etcdrpc.RangeRequest.KEY,
            'key': etcdrpc.RangeRequest.KEY,
            'version': etcdrpc.RangeRequest.VERSION,
            'create': etcdrpc.RangeRequest.CREATE,
            'mod': etcdrpc.RangeRequest.MOD,
            'value': etcdrpc.RangeRequest.VALUE,
        }
        request_sort_target = sort_targets.get(sort_target)
        if request_sort_target is None:
            raise ValueError('sort_target must be one of "key", '
                             '"version", "create", "mod" or "value"')
        range_request.sort_target = request_sort_target

        return range_request

    @_handle_errors
    def get_response(self, key, **kwargs):
        """Get the value of a key from etcd."""
        range_request = self._build_get_range_request(
            key,
            **kwargs
        )

        return self.kvstub.Range(
            range_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    def get(self, key, **kwargs):
        """
        Get the value of a key from etcd.

        example usage:

        .. code-block:: python

            >>> import etcd3
            >>> etcd = etcd3.client()
            >>> etcd.get('/thing/key')
            'hello world'

        :param key: key in etcd to get
        :returns: value of key and metadata
        :rtype: bytes, ``KVMetadata``
        """
        range_response = self.get_response(key, **kwargs)
        if range_response.count < 1:
            return None, None
        else:
            kv = range_response.kvs.pop()
            return kv.value, KVMetadata(kv, range_response.header)

    @_handle_errors
    def get_prefix_response(self, key_prefix, **kwargs):
        """Get a range of keys with a prefix."""
        if any(kwarg in kwargs for kwarg in ("key", "range_end")):
            raise TypeError("Don't use key or range_end with prefix")

        range_request = self._build_get_range_request(
            key=key_prefix,
            range_end=utils.prefix_range_end(utils.to_bytes(key_prefix)),
            **kwargs
        )

        return self.kvstub.Range(
            range_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    def get_prefix(self, key_prefix, **kwargs):
        """
        Get a range of keys with a prefix.

        :param key_prefix: first key in range
        :param keys_only: if True, retrieve only the keys, not the values

        :returns: sequence of (value, metadata) tuples
        """
        range_response = self.get_prefix_response(key_prefix, **kwargs)
        return (
            (kv.value, KVMetadata(kv, range_response.header))
            for kv in range_response.kvs
        )

    @_handle_errors
    def get_range_response(self, range_start, range_end, sort_order=None,
                           sort_target='key', **kwargs):
        """Get a range of keys."""
        range_request = self._build_get_range_request(
            key=range_start,
            range_end=range_end,
            sort_order=sort_order,
            sort_target=sort_target,
            **kwargs
        )

        return self.kvstub.Range(
            range_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    def get_range(self, range_start, range_end, **kwargs):
        """
        Get a range of keys.

        :param range_start: first key in range
        :param range_end: last key in range
        :returns: sequence of (value, metadata) tuples
        """
        range_response = self.get_range_response(range_start, range_end,
                                                 **kwargs)
        for kv in range_response.kvs:
            yield (kv.value, KVMetadata(kv, range_response.header))

    @_handle_errors
    def get_all_response(self, sort_order=None, sort_target='key',
                         keys_only=False):
        """Get all keys currently stored in etcd."""
        range_request = self._build_get_range_request(
            key=b'\0',
            range_end=b'\0',
            sort_order=sort_order,
            sort_target=sort_target,
            keys_only=keys_only,
        )

        return self.kvstub.Range(
            range_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    def get_all(self, **kwargs):
        """
        Get all keys currently stored in etcd.

        :param keys_only: if True, retrieve only the keys, not the values
        :returns: sequence of (value, metadata) tuples
        """
        range_response = self.get_all_response(**kwargs)
        for kv in range_response.kvs:
            yield (kv.value, KVMetadata(kv, range_response.header))

    def _build_put_request(self, key, value, lease=None, prev_kv=False):
        put_request = etcdrpc.PutRequest()
        put_request.key = utils.to_bytes(key)
        put_request.value = utils.to_bytes(value)
        put_request.lease = utils.lease_to_id(lease)
        put_request.prev_kv = prev_kv

        return put_request

    @_handle_errors
    def put(self, key, value, lease=None, prev_kv=False):
        """
        Save a value to etcd.

        Example usage:

        .. code-block:: python

            >>> import etcd3
            >>> etcd = etcd3.client()
            >>> etcd.put('/thing/key', 'hello world')

        :param key: key in etcd to set
        :param value: value to set key to
        :type value: bytes
        :param lease: Lease to associate with this key.
        :type lease: either :class:`.Lease`, or int (ID of lease)
        :param prev_kv: return the previous key-value pair
        :type prev_kv: bool
        :returns: a response containing a header and the prev_kv
        :rtype: :class:`.rpc_pb2.PutResponse`
        """
        put_request = self._build_put_request(key, value, lease=lease,
                                              prev_kv=prev_kv)
        return self.kvstub.Put(
            put_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def put_if_not_exists(self, key, value, lease=None):
        """
        Atomically puts a value only if the key previously had no value.

        This is the etcdv3 equivalent to setting a key with the etcdv2
        parameter prevExist=false.

        :param key: key in etcd to put
        :param value: value to be written to key
        :type value: bytes
        :param lease: Lease to associate with this key.
        :type lease: either :class:`.Lease`, or int (ID of lease)
        :returns: state of transaction, ``True`` if the put was successful,
                  ``False`` otherwise
        :rtype: bool
        """
        status, _ = self.transaction(
            compare=[self.transactions.create(key) == '0'],
            success=[self.transactions.put(key, value, lease=lease)],
            failure=[],
        )

        return status

    @_handle_errors
    def replace(self, key, initial_value, new_value):
        """
        Atomically replace the value of a key with a new value.

        This compares the current value of a key, then replaces it with a new
        value if it is equal to a specified value. This operation takes place
        in a transaction.

        :param key: key in etcd to replace
        :param initial_value: old value to replace
        :type initial_value: bytes
        :param new_value: new value of the key
        :type new_value: bytes
        :returns: status of transaction, ``True`` if the replace was
                  successful, ``False`` otherwise
        :rtype: bool
        """
        status, _ = self.transaction(
            compare=[self.transactions.value(key) == initial_value],
            success=[self.transactions.put(key, new_value)],
            failure=[],
        )

        return status

    def _build_delete_request(self, key,
                              range_end=None,
                              prev_kv=False):
        delete_request = etcdrpc.DeleteRangeRequest()
        delete_request.key = utils.to_bytes(key)
        delete_request.prev_kv = prev_kv

        if range_end is not None:
            delete_request.range_end = utils.to_bytes(range_end)

        return delete_request

    @_handle_errors
    def delete(self, key, prev_kv=False, return_response=False):
        """
        Delete a single key in etcd.

        :param key: key in etcd to delete
        :param prev_kv: return the deleted key-value pair
        :type prev_kv: bool
        :param return_response: return the full response
        :type return_response: bool
        :returns: True if the key has been deleted when
                  ``return_response`` is False and a response containing
                  a header, the number of deleted keys and prev_kvs when
                  ``return_response`` is True
        """
        delete_request = self._build_delete_request(key, prev_kv=prev_kv)
        delete_response = self.kvstub.DeleteRange(
            delete_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        if return_response:
            return delete_response
        return delete_response.deleted >= 1

    @_handle_errors
    def delete_prefix(self, prefix):
        """Delete a range of keys with a prefix in etcd."""
        delete_request = self._build_delete_request(
            prefix,
            range_end=utils.prefix_range_end(utils.to_bytes(prefix))
        )
        return self.kvstub.DeleteRange(
            delete_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def status(self):
        """Get the status of the responding member."""
        status_request = etcdrpc.StatusRequest()
        status_response = self.maintenancestub.Status(
            status_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

        for m in self.members:
            if m.id == status_response.leader:
                leader = m
                break
        else:
            # raise exception?
            leader = None

        return Status(status_response.version,
                      status_response.dbSize,
                      leader,
                      status_response.raftIndex,
                      status_response.raftTerm)

    @_handle_errors
    def add_watch_callback(self, *args, **kwargs):
        """
        Watch a key or range of keys and call a callback on every response.

        If timeout was declared during the client initialization and
        the watch cannot be created during that time the method raises
        a ``WatchTimedOut`` exception.

        :param key: key to watch
        :param callback: callback function

        :returns: watch_id. Later it could be used for cancelling watch.
        """
        try:
            return self.watcher.add_callback(*args, **kwargs)
        except queue.Empty:
            raise exceptions.WatchTimedOut()

    @_handle_errors
    def add_watch_prefix_callback(self, key_prefix, callback, **kwargs):
        """
        Watch a prefix and call a callback on every response.

        If timeout was declared during the client initialization and
        the watch cannot be created during that time the method raises
        a ``WatchTimedOut`` exception.

        :param key_prefix: prefix to watch
        :param callback: callback function

        :returns: watch_id. Later it could be used for cancelling watch.
        """
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))

        return self.add_watch_callback(key_prefix, callback, **kwargs)

    @_handle_errors
    def watch_response(self, key, **kwargs):
        """
        Watch a key.

        Example usage:

        .. code-block:: python

            responses_iterator, cancel = etcd.watch_response('/doot/key')
            for response in responses_iterator:
                print(response)

        :param key: key to watch

        :returns: tuple of ``responses_iterator`` and ``cancel``.
                  Use ``responses_iterator`` to get the watch responses,
                  each of which contains a header and a list of events.
                  Use ``cancel`` to cancel the watch request.
        """
        response_queue = queue.Queue()

        def callback(response):
            response_queue.put(response)

        watch_id = self.add_watch_callback(key, callback, **kwargs)
        canceled = threading.Event()

        def cancel():
            canceled.set()
            response_queue.put(None)
            self.cancel_watch(watch_id)

        @_handle_errors
        def iterator():
            while not canceled.is_set():
                response = response_queue.get()
                if response is None:
                    canceled.set()
                if isinstance(response, Exception):
                    canceled.set()
                    raise response
                if not canceled.is_set():
                    yield response

        return iterator(), cancel

    def watch(self, key, **kwargs):
        """
        Watch a key.

        Example usage:

        .. code-block:: python

            events_iterator, cancel = etcd.watch('/doot/key')
            for event in events_iterator:
                print(event)

        :param key: key to watch

        :returns: tuple of ``events_iterator`` and ``cancel``.
                  Use ``events_iterator`` to get the events of key changes
                  and ``cancel`` to cancel the watch request.
        """
        response_iterator, cancel = self.watch_response(key, **kwargs)
        return utils.response_to_event_iterator(response_iterator), cancel

    def watch_prefix_response(self, key_prefix, **kwargs):
        """
        Watch a range of keys with a prefix.

        :param key_prefix: prefix to watch

        :returns: tuple of ``responses_iterator`` and ``cancel``.
        """
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))
        return self.watch_response(key_prefix, **kwargs)

    def watch_prefix(self, key_prefix, **kwargs):
        """
        Watch a range of keys with a prefix.

        :param key_prefix: prefix to watch

        :returns: tuple of ``events_iterator`` and ``cancel``.
        """
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))
        return self.watch(key_prefix, **kwargs)

    @_handle_errors
    def watch_once_response(self, key, timeout=None, **kwargs):
        """
        Watch a key and stop after the first response.

        If the timeout was specified and response didn't arrive method
        will raise ``WatchTimedOut`` exception.

        :param key: key to watch
        :param timeout: (optional) timeout in seconds.

        :returns: ``WatchResponse``
        """
        response_queue = queue.Queue()

        def callback(response):
            response_queue.put(response)

        watch_id = self.add_watch_callback(key, callback, **kwargs)

        try:
            return response_queue.get(timeout=timeout)
        except queue.Empty:
            raise exceptions.WatchTimedOut()
        finally:
            self.cancel_watch(watch_id)

    def watch_once(self, key, timeout=None, **kwargs):
        """
        Watch a key and stop after the first event.

        If the timeout was specified and event didn't arrive method
        will raise ``WatchTimedOut`` exception.

        :param key: key to watch
        :param timeout: (optional) timeout in seconds.

        :returns: ``Event``
        """
        response = self.watch_once_response(key, timeout=timeout, **kwargs)
        return response.events[0]

    def watch_prefix_once_response(self, key_prefix, timeout=None, **kwargs):
        """
        Watch a range of keys with a prefix and stop after the first response.

        If the timeout was specified and response didn't arrive method
        will raise ``WatchTimedOut`` exception.
        """
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))
        return self.watch_once_response(key_prefix, timeout=timeout, **kwargs)

    def watch_prefix_once(self, key_prefix, timeout=None, **kwargs):
        """
        Watch a range of keys with a prefix and stop after the first event.

        If the timeout was specified and event didn't arrive method
        will raise ``WatchTimedOut`` exception.
        """
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))
        return self.watch_once(key_prefix, timeout=timeout, **kwargs)

    @_handle_errors
    def cancel_watch(self, watch_id):
        """
        Stop watching a key or range of keys.

        :param watch_id: watch_id returned by ``add_watch_callback`` method
        """
        self.watcher.cancel(watch_id)

    def _ops_to_requests(self, ops):
        """
        Return a list of grpc requests.

        Returns list from an input list of etcd3.transactions.{Put, Get,
        Delete, Txn} objects.
        """
        request_ops = []
        for op in ops:
            if isinstance(op, transactions.Put):
                request = self._build_put_request(op.key, op.value,
                                                  op.lease, op.prev_kv)
                request_op = etcdrpc.RequestOp(request_put=request)
                request_ops.append(request_op)

            elif isinstance(op, transactions.Get):
                request = self._build_get_range_request(op.key, op.range_end)
                request_op = etcdrpc.RequestOp(request_range=request)
                request_ops.append(request_op)

            elif isinstance(op, transactions.Delete):
                request = self._build_delete_request(op.key, op.range_end,
                                                     op.prev_kv)
                request_op = etcdrpc.RequestOp(request_delete_range=request)
                request_ops.append(request_op)

            elif isinstance(op, transactions.Txn):
                compare = [c.build_message() for c in op.compare]
                success_ops = self._ops_to_requests(op.success)
                failure_ops = self._ops_to_requests(op.failure)
                request = etcdrpc.TxnRequest(compare=compare,
                                             success=success_ops,
                                             failure=failure_ops)
                request_op = etcdrpc.RequestOp(request_txn=request)
                request_ops.append(request_op)

            else:
                raise Exception(
                    'Unknown request class {}'.format(op.__class__))
        return request_ops

    @_handle_errors
    def transaction(self, compare, success=None, failure=None):
        """
        Perform a transaction.

        Example usage:

        .. code-block:: python

            etcd.transaction(
                compare=[
                    etcd.transactions.value('/doot/testing') == 'doot',
                    etcd.transactions.version('/doot/testing') > 0,
                ],
                success=[
                    etcd.transactions.put('/doot/testing', 'success'),
                ],
                failure=[
                    etcd.transactions.put('/doot/testing', 'failure'),
                ]
            )

        :param compare: A list of comparisons to make
        :param success: A list of operations to perform if all the comparisons
                        are true
        :param failure: A list of operations to perform if any of the
                        comparisons are false
        :return: A tuple of (operation status, responses)
        """
        compare = [c.build_message() for c in compare]

        success_ops = self._ops_to_requests(success)
        failure_ops = self._ops_to_requests(failure)

        transaction_request = etcdrpc.TxnRequest(compare=compare,
                                                 success=success_ops,
                                                 failure=failure_ops)
        txn_response = self.kvstub.Txn(
            transaction_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

        responses = []
        for response in txn_response.responses:
            response_type = response.WhichOneof('response')
            if response_type in ['response_put', 'response_delete_range',
                                 'response_txn']:
                responses.append(response)

            elif response_type == 'response_range':
                range_kvs = []
                for kv in response.response_range.kvs:
                    range_kvs.append((kv.value,
                                      KVMetadata(kv, txn_response.header)))

                responses.append(range_kvs)

        return txn_response.succeeded, responses

    @_handle_errors
    def lease(self, ttl, lease_id=None):
        """
        Create a new lease.

        All keys attached to this lease will be expired and deleted if the
        lease expires. A lease can be sent keep alive messages to refresh the
        ttl.

        :param ttl: Requested time to live
        :param lease_id: Requested ID for the lease

        :returns: new lease
        :rtype: :class:`.Lease`
        """
        lease_grant_request = etcdrpc.LeaseGrantRequest(TTL=ttl, ID=lease_id)
        lease_grant_response = self.leasestub.LeaseGrant(
            lease_grant_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        return leases.Lease(lease_id=lease_grant_response.ID,
                            ttl=lease_grant_response.TTL,
                            etcd_client=self)

    @_handle_errors
    def revoke_lease(self, lease_id):
        """
        Revoke a lease.

        :param lease_id: ID of the lease to revoke.
        """
        lease_revoke_request = etcdrpc.LeaseRevokeRequest(ID=lease_id)
        self.leasestub.LeaseRevoke(
            lease_revoke_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def refresh_lease(self, lease_id):
        keep_alive_request = etcdrpc.LeaseKeepAliveRequest(ID=lease_id)
        request_stream = [keep_alive_request]
        for response in self.leasestub.LeaseKeepAlive(
                iter(request_stream),
                self.timeout,
                credentials=self.call_credentials,
                metadata=self.metadata):
            yield response

    @_handle_errors
    def get_lease_info(self, lease_id):
        # only available in etcd v3.1.0 and later
        ttl_request = etcdrpc.LeaseTimeToLiveRequest(ID=lease_id,
                                                     keys=True)
        return self.leasestub.LeaseTimeToLive(
            ttl_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def lock(self, name, ttl=60):
        """
        Create a new lock.

        :param name: name of the lock
        :type name: string or bytes
        :param ttl: length of time for the lock to live for in seconds. The
                    lock will be released after this time elapses, unless
                    refreshed
        :type ttl: int
        :returns: new lock
        :rtype: :class:`.Lock`
        """
        return locks.Lock(name, ttl=ttl, etcd_client=self)

    @_handle_errors
    def add_member(self, urls):
        """
        Add a member into the cluster.

        :returns: new member
        :rtype: :class:`.Member`
        """
        member_add_request = etcdrpc.MemberAddRequest(peerURLs=urls)

        member_add_response = self.clusterstub.MemberAdd(
            member_add_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

        member = member_add_response.member
        return etcd3.members.Member(member.ID,
                                    member.name,
                                    member.peerURLs,
                                    member.clientURLs,
                                    etcd_client=self)

    @_handle_errors
    def remove_member(self, member_id):
        """
        Remove an existing member from the cluster.

        :param member_id: ID of the member to remove
        """
        member_rm_request = etcdrpc.MemberRemoveRequest(ID=member_id)
        self.clusterstub.MemberRemove(
            member_rm_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def update_member(self, member_id, peer_urls):
        """
        Update the configuration of an existing member in the cluster.

        :param member_id: ID of the member to update
        :param peer_urls: new list of peer urls the member will use to
                          communicate with the cluster
        """
        member_update_request = etcdrpc.MemberUpdateRequest(ID=member_id,
                                                            peerURLs=peer_urls)
        self.clusterstub.MemberUpdate(
            member_update_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @property
    @_handle_errors
    def members(self):
        """
        List of all members associated with the cluster.

        :type: sequence of :class:`.Member`

        """
        member_list_request = etcdrpc.MemberListRequest()
        member_list_response = self.clusterstub.MemberList(
            member_list_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

        for member in member_list_response.members:
            yield etcd3.members.Member(member.ID,
                                       member.name,
                                       member.peerURLs,
                                       member.clientURLs,
                                       etcd_client=self)

    @_handle_errors
    def compact(self, revision, physical=False):
        """
        Compact the event history in etcd up to a given revision.

        All superseded keys with a revision less than the compaction revision
        will be removed.

        :param revision: revision for the compaction operation
        :param physical: if set to True, the request will wait until the
                         compaction is physically applied to the local database
                         such that compacted entries are totally removed from
                         the backend database
        """
        compact_request = etcdrpc.CompactionRequest(revision=revision,
                                                    physical=physical)
        self.kvstub.Compact(
            compact_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def defragment(self):
        """Defragment a member's backend database to recover storage space."""
        defrag_request = etcdrpc.DefragmentRequest()
        self.maintenancestub.Defragment(
            defrag_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def hash(self):
        """
        Return the hash of the local KV state.

        :returns: kv state hash
        :rtype: int
        """
        hash_request = etcdrpc.HashRequest()
        return self.maintenancestub.Hash(hash_request).hash

    def _build_alarm_request(self, alarm_action, member_id, alarm_type):
        alarm_request = etcdrpc.AlarmRequest()

        if alarm_action == 'get':
            alarm_request.action = etcdrpc.AlarmRequest.GET
        elif alarm_action == 'activate':
            alarm_request.action = etcdrpc.AlarmRequest.ACTIVATE
        elif alarm_action == 'deactivate':
            alarm_request.action = etcdrpc.AlarmRequest.DEACTIVATE
        else:
            raise ValueError('Unknown alarm action: {}'.format(alarm_action))

        alarm_request.memberID = member_id

        if alarm_type == 'none':
            alarm_request.alarm = etcdrpc.NONE
        elif alarm_type == 'no space':
            alarm_request.alarm = etcdrpc.NOSPACE
        else:
            raise ValueError('Unknown alarm type: {}'.format(alarm_type))

        return alarm_request

    @_handle_errors
    def create_alarm(self, member_id=0):
        """Create an alarm.

        If no member id is given, the alarm is activated for all the
        members of the cluster. Only the `no space` alarm can be raised.

        :param member_id: The cluster member id to create an alarm to.
                          If 0, the alarm is created for all the members
                          of the cluster.
        :returns: list of :class:`.Alarm`
        """
        alarm_request = self._build_alarm_request('activate',
                                                  member_id,
                                                  'no space')
        alarm_response = self.maintenancestub.Alarm(
            alarm_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

        return [Alarm(alarm.alarm, alarm.memberID)
                for alarm in alarm_response.alarms]

    @_handle_errors
    def list_alarms(self, member_id=0, alarm_type='none'):
        """List the activated alarms.

        :param member_id:
        :param alarm_type: The cluster member id to create an alarm to.
                           If 0, the alarm is created for all the members
                           of the cluster.
        :returns: sequence of :class:`.Alarm`
        """
        alarm_request = self._build_alarm_request('get',
                                                  member_id,
                                                  alarm_type)
        alarm_response = self.maintenancestub.Alarm(
            alarm_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

        for alarm in alarm_response.alarms:
            yield Alarm(alarm.alarm, alarm.memberID)

    @_handle_errors
    def disarm_alarm(self, member_id=0):
        """Cancel an alarm.

        :param member_id: The cluster member id to cancel an alarm.
                          If 0, the alarm is canceled for all the members
                          of the cluster.
        :returns: List of :class:`.Alarm`
        """
        alarm_request = self._build_alarm_request('deactivate',
                                                  member_id,
                                                  'no space')
        alarm_response = self.maintenancestub.Alarm(
            alarm_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

        return [Alarm(alarm.alarm, alarm.memberID)
                for alarm in alarm_response.alarms]

    @_handle_errors
    def snapshot(self, file_obj):
        """Take a snapshot of the database.

        :param file_obj: A file-like object to write the database contents in.
        """
        snapshot_request = etcdrpc.SnapshotRequest()
        snapshot_response = self.maintenancestub.Snapshot(
            snapshot_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

        for response in snapshot_response:
            file_obj.write(response.blob)

    @_handle_errors
    def enable_auth(self):
        """Enable authentication in etcd."""
        req = etcdrpc.AuthEnableRequest()
        self.auth_stub.AuthEnable(
            req,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def disable_auth(self):
        """Disable authentication in etcd."""
        req = etcdrpc.AuthDisableRequest()
        self.auth_stub.AuthDisable(
            req,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @property
    @_handle_errors
    def roles(self):
        """List of all roles within the cluster.

        :type: sequence of str

        """
        role_list_request = etcdrpc.AuthRoleListRequest()
        resp = self.auth_stub.RoleList(
            role_list_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        return frozenset(resp.roles)

    @_handle_errors
    def add_role(self, name):
        """Add a new Auth Role to the the cluster.

        :param name: Role name to add

        """
        req = etcdrpc.AuthRoleAddRequest()
        req.name = name
        self.auth_stub.RoleAdd(
            req,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def get_role(self, role):
        """Retrieve a role from the cluster.

        :param role: Name of the role to fetch

        :returns: :class:`etcd3.role.Role` object

        """
        req = etcdrpc.AuthRoleGetRequest()
        req.role = role
        resp = self.auth_stub.RoleGet(
            req,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        return etcd3.role.Role(role, resp.perm, self)

    @_handle_errors
    def revoke_role(self, user, role):
        """Revoke a specific role from a user.

        :param user: Name of the user to remove the role from
        :param role: Name of the role to revoke

        """
        req = etcdrpc.AuthUserRevokeRoleRequest()
        req.name = user
        req.role = role
        self.auth_stub.UserRevokeRole(
            req,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def grant_role(self, user, role):
        """Grant a specific role to a user.

        :param user: Name of the user to grant the role to
        :param role: Name of the role to grant

        """
        req = etcdrpc.AuthUserGrantRoleRequest()
        req.user = user
        req.role = role
        self.auth_stub.UserGrantRole(
            req,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def delete_role(self, role):
        """Delete a user from the cluster.

        :param user: User name to delete

        """
        req = etcdrpc.AuthRoleDeleteRequest()
        req.role = role
        self.auth_stub.RoleDelete(
            req,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def grant_permission_role(self, role, perm, key, end=None, prefix=False):
        """Assign a permissionto a role.

        :param role: Name of the role to grant permission to
        :param perm: Read ``etcd3.Perms.r`` Write ``etcd3.Perms.w``, or
                           Read/Write ``etcd3.Perms.rw``
        :param key: Key/heiarchy to grant the permission for
        :param end: Used to set a range of keys
        :param prefix: (Boolean) If True and "end" is not set add one to the
                       last byte and use it for end

        """
        if prefix and end is None:
            end = utils.prefix_range_end(utils.to_bytes(key))

        if perm not in Perms:
            raise ValueError("Invalid permission: %s" % repr(perm))

        req = etcdrpc.AuthRoleGrantPermissionRequest()
        req.name = role
        req.perm.permType = perm
        req.perm.key = utils.to_bytes(key)
        if end is not None:
            req.perm.range_end = utils.to_bytes(end)
        self.auth_stub.RoleGrantPermission(
            req,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def revoke_permission_role(self, role, key, end=None, prefix=False):
        """Revoke a permission from a role.

        :param role: Name of the role to grant permission to
        :param perm: Read ``etcd3.Perms.r`` Write ``etcd3.Perms.w``, or
                           Read/Write ``etcd3.Perms.rw``
        :param key: Key/heiarchy to grant the permission for
        :param end: Used to set a range of keys
        :param prefix: (Boolean) If True and "end" is not set add one to the
                       last byte and use it for end

        """
        if prefix and end is None:
            end = utils.prefix_range_end(utils.to_bytes(key))
        req = etcdrpc.AuthRoleRevokePermissionRequest()
        if end is not None:
            req.range_end = utils.to_bytes(end)
        req.role = role
        req.key = utils.to_bytes(key)
        self.auth_stub.RoleRevokePermission(
            req,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @property
    @_handle_errors
    def users(self):
        """List of all users associated with the cluster.

        :type: frozenset of str

        """
        req = etcdrpc.AuthUserListRequest()
        resp = self.auth_stub.UserList(
            req,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        return frozenset(resp.users)

    @_handle_errors
    def add_user(self, user, passwd, use_password=True):
        """Add a new user to the the cluster.

        :param user: User name to for the new user
        :param passwd: Password for the usee

        """
        req = etcdrpc.AuthUserAddRequest()
        req.name = user
        req.password = passwd
        req.options.no_password = not use_password
        self.auth_stub.UserAdd(
            req,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def get_user(self, user):
        """Add a new user to the the cluster.

        :param user: Name of the user to fetch
        :returns: :class:`etcd3.user.User` object

        """
        req = etcdrpc.AuthUserGetRequest()
        req.name = user
        resp = self.auth_stub.UserGet(
            req,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        return etcd3.user.User(user, resp.roles, self)

    @_handle_errors
    def delete_user(self, user):
        """Delete a user from the cluster.

        :param user: User name to delete

        """
        req = etcdrpc.AuthUserDeleteRequest()
        req.name = user
        self.auth_stub.UserDelete(
            req,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    def change_password(self, user, password):
        """Change the password for a user.

        :param user: User name to modify
        :param password: New password to set

        """
        req = etcdrpc.AuthUserChangePasswordRequest()
        req.name = user
        req.password = password
        self.auth_stub.UserChangePassword(
            req,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )


def client(host='localhost', port=2379,
           ca_cert=None, cert_key=None, cert_cert=None, timeout=None,
           user=None, password=None, grpc_options=None):
    """Return an instance of an Etcd3Client."""
    return Etcd3Client(host=host,
                       port=port,
                       ca_cert=ca_cert,
                       cert_key=cert_key,
                       cert_cert=cert_cert,
                       timeout=timeout,
                       user=user,
                       password=password,
                       grpc_options=grpc_options)
