import functools
import inspect
import threading

import grpc
import grpc._channel

from six.moves import queue

import etcd3.etcdrpc as etcdrpc
import etcd3.exceptions as exceptions
import etcd3.leases as leases
import etcd3.locks as locks
import etcd3.members
import etcd3.transactions as transactions
import etcd3.utils as utils
import etcd3.watch as watch

_EXCEPTIONS_BY_CODE = {
    grpc.StatusCode.INTERNAL: exceptions.InternalServerError,
    grpc.StatusCode.UNAVAILABLE: exceptions.ConnectionFailedError,
    grpc.StatusCode.DEADLINE_EXCEEDED: exceptions.ConnectionTimeoutError,
    grpc.StatusCode.FAILED_PRECONDITION: exceptions.PreconditionFailedError,
}


def _handle_errors(f):
    @functools.wraps(f)
    def handler(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except grpc.RpcError as exc:
            code = exc.code()
            exception = _EXCEPTIONS_BY_CODE.get(code)
            if exception is None:
                raise
            raise exception

    @functools.wraps(f)
    def generator_handler(*args, **kwargs):
        try:
            for data in f(*args, **kwargs):
                yield data
        except grpc.RpcError as exc:
            code = exc.code()
            exception = _EXCEPTIONS_BY_CODE.get(code)
            if exception is None:
                raise
            raise exception

    if inspect.isgeneratorfunction(f):
        return generator_handler
    else:
        return handler


class Transactions(object):
    def __init__(self):
        self.value = transactions.Value
        self.version = transactions.Version
        self.create = transactions.Create
        self.mod = transactions.Mod

        self.put = transactions.Put
        self.get = transactions.Get
        self.delete = transactions.Delete


class KVMetadata(object):
    def __init__(self, keyvalue):
        self.key = keyvalue.key
        self.create_revision = keyvalue.create_revision
        self.mod_revision = keyvalue.mod_revision
        self.version = keyvalue.version
        self.lease_id = keyvalue.lease


class Status(object):
    def __init__(self, version, db_size, leader, raft_index, raft_term):
        self.version = version
        self.db_size = db_size
        self.leader = leader
        self.raft_index = raft_index
        self.raft_term = raft_term


class Etcd3Client(object):
    def __init__(self, host='localhost', port=2379,
                 ca_cert=None, cert_key=None, cert_cert=None, timeout=None):
        self._url = '{host}:{port}'.format(host=host, port=port)

        cert_params = [c is not None for c in (cert_cert, cert_key, ca_cert)]
        if all(cert_params):
            # all the cert parameters are set
            credentials = self._get_secure_creds(ca_cert,
                                                 cert_key,
                                                 cert_cert)
            self.uses_secure_channel = True
            self.channel = grpc.secure_channel(self._url, credentials)
        elif any(cert_params):
            # some of the cert parameters are set
            raise ValueError('the parameters cert_cert, cert_key and ca_cert '
                             'must all be set to use a secure channel')
        else:
            self.uses_secure_channel = False
            self.channel = grpc.insecure_channel(self._url)

        self.timeout = timeout
        self.kvstub = etcdrpc.KVStub(self.channel)
        self.watcher = watch.Watcher(etcdrpc.WatchStub(self.channel),
                                     timeout=self.timeout)
        self.clusterstub = etcdrpc.ClusterStub(self.channel)
        self.leasestub = etcdrpc.LeaseStub(self.channel)
        self.maintenancestub = etcdrpc.MaintenanceStub(self.channel)
        self.transactions = Transactions()

    def _get_secure_creds(self, ca_cert, cert_key, cert_cert):
        with open(ca_cert, 'rb') as ca_cert_file:
            with open(cert_key, 'rb') as cert_key_file:
                with open(cert_cert, 'rb') as cert_cert_file:
                    return grpc.ssl_channel_credentials(
                        ca_cert_file.read(),
                        cert_key_file.read(),
                        cert_cert_file.read()
                    )

    def _build_get_range_request(self, key,
                                 range_end=None,
                                 limit=None,
                                 revision=None,
                                 sort_order=None,
                                 sort_target='key',
                                 serializable=None,
                                 keys_only=None,
                                 count_only=None,
                                 min_mod_revision=None,
                                 max_mod_revision=None,
                                 min_create_revision=None,
                                 max_create_revision=None):
        range_request = etcdrpc.RangeRequest()
        range_request.key = utils.to_bytes(key)
        if range_end is not None:
            range_request.range_end = utils.to_bytes(range_end)

        if sort_order is None:
            range_request.sort_order = etcdrpc.RangeRequest.NONE
        elif sort_order == 'ascend':
            range_request.sort_order = etcdrpc.RangeRequest.ASCEND
        elif sort_order == 'descend':
            range_request.sort_order = etcdrpc.RangeRequest.DESCEND
        else:
            raise ValueError('unknown sort order: "{}"'.format(sort_order))

        if sort_target is None or sort_target == 'key':
            range_request.sort_target = etcdrpc.RangeRequest.KEY
        elif sort_target == 'version':
            range_request.sort_target = etcdrpc.RangeRequest.VERSION
        elif sort_target == 'create':
            range_request.sort_target = etcdrpc.RangeRequest.CREATE
        elif sort_target == 'mod':
            range_request.sort_target = etcdrpc.RangeRequest.MOD
        elif sort_target == 'value':
            range_request.sort_target = etcdrpc.RangeRequest.VALUE
        else:
            raise ValueError('sort_target must be one of "key", '
                             '"version", "create", "mod" or "value"')

        return range_request

    @_handle_errors
    def get(self, key):
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
        range_request = self._build_get_range_request(key)
        range_response = self.kvstub.Range(range_request, self.timeout)

        if range_response.count < 1:
            return None, None
        else:
            kv = range_response.kvs.pop()
            return kv.value, KVMetadata(kv)

    @_handle_errors
    def get_prefix(self, key_prefix, sort_order=None, sort_target='key'):
        """
        Get a range of keys with a prefix.

        :param key_prefix: first key in range

        :returns: sequence of (value, metadata) tuples
        """
        range_request = self._build_get_range_request(
            key=key_prefix,
            range_end=utils.increment_last_byte(utils.to_bytes(key_prefix)),
            sort_order=sort_order,
        )

        range_response = self.kvstub.Range(range_request, self.timeout)

        if range_response.count < 1:
            return
        else:
            for kv in range_response.kvs:
                yield (kv.value, KVMetadata(kv))

    @_handle_errors
    def get_all(self, sort_order=None, sort_target='key'):
        """
        Get all keys currently stored in etcd.

        :returns: sequence of (value, metadata) tuples
        """
        range_request = self._build_get_range_request(
            key=b'\0',
            range_end=b'\0',
            sort_order=sort_order,
            sort_target=sort_target,
        )

        range_response = self.kvstub.Range(range_request, self.timeout)

        if range_response.count < 1:
            return
        else:
            for kv in range_response.kvs:
                yield (kv.value, KVMetadata(kv))

    def _build_put_request(self, key, value, lease=None):
        put_request = etcdrpc.PutRequest()
        put_request.key = utils.to_bytes(key)
        put_request.value = utils.to_bytes(value)
        put_request.lease = utils.lease_to_id(lease)
        return put_request

    @_handle_errors
    def put(self, key, value, lease=None):
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
        """
        put_request = self._build_put_request(key, value, lease=lease)
        self.kvstub.Put(put_request, self.timeout)

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
                              prev_kv=None):
        delete_request = etcdrpc.DeleteRangeRequest()
        delete_request.key = utils.to_bytes(key)

        if range_end is not None:
            delete_request.range_end = utils.to_bytes(range_end)

        if prev_kv is not None:
            delete_request.prev_kv = prev_kv

        return delete_request

    @_handle_errors
    def delete(self, key):
        """
        Delete a single key in etcd.

        :param key: key in etcd to delete
        """
        delete_request = self._build_delete_request(key)
        self.kvstub.DeleteRange(delete_request, self.timeout)

    @_handle_errors
    def delete_prefix(self, prefix):
        """Delete a range of keys with a prefix in etcd."""
        delete_request = self._build_delete_request(
            prefix,
            range_end=utils.increment_last_byte(utils.to_bytes(prefix))
        )
        return self.kvstub.DeleteRange(delete_request, self.timeout)

    @_handle_errors
    def status(self):
        """Get the status of the responding member."""
        status_request = etcdrpc.StatusRequest()
        status_response = self.maintenancestub.Status(status_request)

        for m in self.members:
            if m.id == status_response.leader:
                leader = m
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
        Watch a key or range of keys and call a callback on every event.

        :param key: key to watch
        :param callback: callback function

        :returns: watch_id. Later it could be used for cancelling watch.
        """
        return self.watcher.add_callback(*args, **kwargs)

    @_handle_errors
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
                  and ``cancel`` to cancel the watch request
        """
        event_queue = queue.Queue()

        def callback(event):
            event_queue.put(event)

        watch_id = self.add_watch_callback(key, callback, **kwargs)
        canceled = threading.Event()

        def cancel():
            canceled.set()
            event_queue.put(None)
            self.cancel_watch(watch_id)

        def iterator():
            while not canceled.is_set():
                event = event_queue.get()
                if event is None:
                    canceled.set()
                if not canceled.is_set():
                    yield event

        return iterator(), cancel

    @_handle_errors
    def watch_prefix(self, key_prefix, **kwargs):
        """The same as ``watch``, but watches a range of keys with a prefix."""
        kwargs['range_end'] = \
            utils.increment_last_byte(utils.to_bytes(key_prefix))
        return self.watch(key_prefix, **kwargs)

    @_handle_errors
    def watch_once(self, key, timeout=None, **kwargs):
        """
        Watch a key and stops after the first event.

        If the timeout was specified and event didn't arrived method
        will raise ``WatchTimedOut`` exception.

        :param key: key to watch
        :param timeout: (optional) timeout in seconds.
        :returns: ``Event``
        """
        event_queue = queue.Queue()

        def callback(event):
            event_queue.put(event)

        watch_id = self.add_watch_callback(key, callback, **kwargs)

        try:
            return event_queue.get(timeout=timeout)
        except queue.Empty:
            raise exceptions.WatchTimedOut()
        finally:
            self.cancel_watch(watch_id)

    @_handle_errors
    def watch_prefix_once(self, key_prefix, timeout=None, **kwargs):
        """
        The same as ``watch_once``, but watches a range of keys with a prefix.

        If the timeout was specified and event didn't arrived method
        will raise ``WatchTimedOut`` exception.
        """
        kwargs['range_end'] = \
            utils.increment_last_byte(utils.to_bytes(key_prefix))
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
        Delete} objects.
        """
        request_ops = []
        for op in ops:
            if isinstance(op, transactions.Put):
                request = self._build_put_request(op.key, op.value, op.lease)
                request_op = etcdrpc.RequestOp(request_put=request)
                request_ops.append(request_op)

            elif isinstance(op, transactions.Get):
                request = self._build_get_range_request(op.key)
                request_op = etcdrpc.RequestOp(request_range=request)
                request_ops.append(request_op)

            elif isinstance(op, transactions.Delete):
                request = self._build_delete_request(op.key)
                request_op = etcdrpc.RequestOp(request_delete_range=request)
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
        """
        compare = [c.build_message() for c in compare]

        success_ops = self._ops_to_requests(success)
        failure_ops = self._ops_to_requests(failure)

        transaction_request = etcdrpc.TxnRequest(compare=compare,
                                                 success=success_ops,
                                                 failure=failure_ops)
        txn_response = self.kvstub.Txn(transaction_request, self.timeout)

        responses = []
        for response in txn_response.responses:
            response_type = response.WhichOneof('response')
            if response_type == 'response_put':
                responses.append(None)

            elif response_type == 'response_range':
                range_kvs = []
                for kv in response.response_range.kvs:
                    range_kvs.append((kv.value, KVMetadata(kv)))

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
        lease_grant_response = self.leasestub.LeaseGrant(lease_grant_request,
                                                         self.timeout)
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
        self.leasestub.LeaseRevoke(lease_revoke_request, self.timeout)

    @_handle_errors
    def refresh_lease(self, lease_id):
        keep_alive_request = etcdrpc.LeaseKeepAliveRequest(ID=lease_id)
        request_stream = [keep_alive_request]
        for response in self.leasestub.LeaseKeepAlive(iter(request_stream),
                                                      self.timeout):
            yield response

    @_handle_errors
    def get_lease_info(self, lease_id):
        # only available in etcd v3.1.0 and later
        ttl_request = etcdrpc.LeaseTimeToLiveRequest(ID=lease_id,
                                                     keys=True)
        return self.leasestub.LeaseTimeToLive(ttl_request, self.timeout)

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

        member_add_response = self.clusterstub.MemberAdd(member_add_request,
                                                         self.timeout)
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
        self.clusterstub.MemberRemove(member_rm_request, self.timeout)

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
        self.clusterstub.MemberUpdate(member_update_request, self.timeout)

    @property
    def members(self):
        """
        List of all members associated with the cluster.

        :type: sequence of :class:`.Member`

        """
        member_list_request = etcdrpc.MemberListRequest()
        member_list_response = self.clusterstub.MemberList(member_list_request,
                                                           self.timeout)

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
        self.kvstub.Compact(compact_request, self.timeout)

    @_handle_errors
    def defragment(self):
        """Defragment a member's backend database to recover storage space."""
        defrag_request = etcdrpc.DefragmentRequest()
        self.maintenancestub.Defragment(defrag_request)


def client(host='localhost', port=2379,
           ca_cert=None, cert_key=None, cert_cert=None, timeout=None):
    """Return an instance of an Etcd3Client."""
    return Etcd3Client(host=host,
                       port=port,
                       ca_cert=ca_cert,
                       cert_key=cert_key,
                       cert_cert=cert_cert,
                       timeout=timeout)
