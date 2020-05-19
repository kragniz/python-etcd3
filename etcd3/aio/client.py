import asyncio
import functools
import inspect
import warnings

from aiofiles import open
import aiogrpc
from grpclib.client import Channel
import grpc

import etcd3.aio.leases as leases
import etcd3.aio.locks as locks
import etcd3.aio.members
import etcd3.aio.watch as watch
import etcd3.aio.etcdrpc as etcdrpc
import etcd3.exceptions as exceptions
import etcd3.utils as utils
import etcd3.aio.transactions as transactions

_EXCEPTIONS_BY_CODE = {
    grpc.StatusCode.INTERNAL: exceptions.InternalServerError,
    grpc.StatusCode.UNAVAILABLE: exceptions.ConnectionFailedError,
    grpc.StatusCode.DEADLINE_EXCEEDED: exceptions.ConnectionTimeoutError,
    grpc.StatusCode.FAILED_PRECONDITION: exceptions.PreconditionFailedError,
}

def _translate_exception(exc):
    code = exc.code()
    exception = _EXCEPTIONS_BY_CODE.get(code)
    if exception is None:
        raise
    raise exception


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

def _handle_errors(f):  # noqa: C901
    if inspect.isasyncgenfunction(f):
        async def handler(*args, **kwargs):
            try:
                async for data in f(*args, **kwargs):
                    yield data
            except grpc.RpcError as exc:
                _translate_exception(exc)
    elif inspect.iscoroutinefunction(f):
        async def handler(*args, **kwargs):
            try:
                return await f(*args, **kwargs)
            except grpc.RpcError as exc:
                _translate_exception(exc)
    else:
        def handler(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except grpc.RpcError as exc:
                _translate_exception(exc)

    return functools.wraps(f)(handler)


def _ensure_channel(f):
    if inspect.isasyncgenfunction(f):
        async def handler(*args, **kwargs):
            await args[0].open()
            async for data in f(*args, **kwargs):
                yield data
    elif inspect.iscoroutinefunction(f):
        async def handler(*args, **kwargs):
            await args[0].open()
            return await f(*args, **kwargs)
    else:
        raise TypeError

    return functools.wraps(f)(handler)


class Etcd3Client:
    def __init__(self, host='localhost', port=2379,
                 ca_cert=None, cert_key=None, cert_cert=None, timeout=None,
                 user=None, password=None, grpc_options=None,
                 loop=None):

        self.host = host
        self.port = port
        self.timeout = timeout
        self.call_credentials = None
        self.transactions = Transactions()

        if grpc_options:
            warnings.warn("grpc_options can't be used with asyncio backend")

        cert_params = (cert_cert, cert_key)
        if any(cert_params) and None in cert_params:
            raise ValueError(
                'to use a secure channel ca_cert is required by itself, '
                'or cert_cert and cert_key must both be specified.')

        cred_params = (user, password)
        if any(cred_params) and None in cred_params:
            raise Exception(
                'if using authentication credentials both user and password '
                'must be specified.'
            )

        self.ca_cert = ca_cert
        self.cert_key = cert_key
        self.cert_cert = cert_cert
        self.user = user
        self.password = password
        self.loop = loop

        self._init_channel_attrs()

    def _init_channel_attrs(self):
        # These attributes will be assigned during opening of GRPC channel
        self.channel = None
        self.metadata = None
        self.uses_secure_channel = None
        self.auth_stub = None
        self.kvstub = None
        self.watcher = None
        self.clusterstub = None
        self.leasestub = None
        self.maintenancestub = None

    async def open(self):
        """Opens GRPC channel"""

        if self.channel:
            return

        cert_params = [c is not None for c in (self.cert_cert, self.cert_key)]
        if self.ca_cert is not None:
            if all(cert_params):
                credentials = await self._get_secure_creds(
                    self.ca_cert,
                    self.cert_key,
                    self.cert_cert
                )
                self.uses_secure_channel = True
                # self.channel = aiogrpc.secure_channel(self._url, credentials,
                #                                       loop=self.loop)
                self.channel = Channel(host=self.host, port=self.port, loop=self.loop)
            else:
                credentials = await self._get_secure_creds(self.ca_cert, None, None)
                self.uses_secure_channel = True
                # self.channel = aiogrpc.secure_channel(self._url, credentials,
                #                                       options=self.grpc_options,
                #                                       loop=self.loop)
                self.channel = Channel(host=self.host, port=self.port, loop=self.loop)

        else:
            self.uses_secure_channel = False
            self.channel = Channel(host=self.host, port=self.port, loop=self.loop)

        cred_params = [c is not None for c in (self.user, self.password)]
        if all(cred_params):
            self.auth_stub = etcdrpc.AuthStub(self.channel)
            auth_request = etcdrpc.AuthenticateRequest(
                name=self.user,
                password=self.password
            )

            resp = await self.auth_stub.Authenticate(auth_request, self.timeout)
            self.metadata = (('token', resp.token),)
            self.call_credentials = grpc.metadata_call_credentials(
                EtcdTokenCallCredentials(resp.token))

        self.kvstub = etcdrpc.KVStub(self.channel)
        self.watcher = watch.Watcher(etcdrpc.WatchStub(self.channel), timeout=self.timeout)
        self.clusterstub = etcdrpc.ClusterStub(self.channel)
        self.leasestub = etcdrpc.LeaseStub(self.channel)
        self.maintenancestub = etcdrpc.MaintenanceStub(self.channel)

    async def close(self):
        """Call the GRPC channel close semantics."""
        if self.channel:
            self.channel.close()
            self._init_channel_attrs()

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, *args):
        await self.close()

    @staticmethod
    async def _get_secure_creds(ca_cert, cert_key=None, cert_cert=None):
        cert_key_file = None
        cert_cert_file = None

        async with open(ca_cert, 'rb') as f:
            ca_cert_file = await f.read()

        if cert_key is not None:
            async with open(cert_key, 'rb') as f:
                cert_key_file = await f.read()

        if cert_cert is not None:
            async with open(cert_cert, 'rb') as f:
                cert_cert_file = await f.read()

        return grpc.ssl_channel_credentials(
            ca_cert_file,
            cert_key_file,
            cert_cert_file
        )

    @_handle_errors
    @_ensure_channel
    async def get(self, key, serializable=False):
        """
        Get the value of a key from etcd.

        example usage:

        .. code-block:: python

            >>> import etcd3
            >>> etcd = etcd3.client()
            >>> etcd.get('/thing/key')
            'hello world'

        :param key: key in etcd to get
        :param serializable: whether to allow serializable reads. This can
            result in stale reads
        :returns: value of key and metadata
        :rtype: bytes, ``KVMetadata``
        """
        range_request = self._build_get_range_request(
            key,
            serializable=serializable)
        range_response = await self.kvstub.Range(
            range_request,
            timeout=self.timeout,
            metadata=self.metadata
        )

        if range_response.count < 1:
            return None, None
        else:
            kv = range_response.kvs.pop()
            return kv.value, KVMetadata(kv, range_response.header)

    @_handle_errors
    @_ensure_channel
    async def get_prefix(self, key_prefix, sort_order=None, sort_target='key',
                         keys_only=False):
        """
        Get a range of keys with a prefix.

        :param key_prefix: first key in range

        :returns: sequence of (value, metadata) tuples
        """
        range_request = self._build_get_range_request(
            key=key_prefix,
            range_end=utils.prefix_range_end(utils.to_bytes(key_prefix)),
            sort_order=sort_order,
            sort_target=sort_target,
            keys_only=keys_only,
        )

        range_response = await self.kvstub.Range(
            range_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

        if range_response.count < 1:
            return
        else:
            for kv in range_response.kvs:
                yield (kv.value, KVMetadata(kv, range_response.header))

    @_handle_errors
    @_ensure_channel
    async def get_range(self, range_start, range_end, sort_order=None,
                        sort_target='key', **kwargs):
        """
        Get a range of keys.

        :param range_start: first key in range
        :param range_end: last key in range
        :returns: sequence of (value, metadata) tuples
        """
        range_request = self._build_get_range_request(
            key=range_start,
            range_end=range_end,
            sort_order=sort_order,
            sort_target=sort_target,
            **kwargs
        )

        range_response = await self.kvstub.Range(
            range_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

        if range_response.count < 1:
            return
        else:
            for kv in range_response.kvs:
                yield (kv.value, KVMetadata(kv, range_response.header))

    @_handle_errors
    @_ensure_channel
    async def get_all(self, sort_order=None, sort_target='key',
                      keys_only=False):
        """
        Get all keys currently stored in etcd.

        :returns: sequence of (value, metadata) tuples
        """
        range_request = self._build_get_range_request(
            key=b'\0',
            range_end=b'\0',
            sort_order=sort_order,
            sort_target=sort_target,
            keys_only=keys_only,
        )

        range_response = await self.kvstub.Range(
            range_request,
            self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

        if range_response.count < 1:
            return
        else:
            for kv in range_response.kvs:
                yield (kv.value, KVMetadata(kv, range_response.header))

    @_handle_errors
    @_ensure_channel
    async def put(self, key, value, lease=None, prev_kv=False):
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
        return await self.kvstub.Put(
            put_request,
            timeout=self.timeout,
            metadata=self.metadata
        )

    @_handle_errors
    @_ensure_channel
    async def replace(self, key, initial_value, new_value):
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
        status, _ = await self.transaction(
            compare=[self.transactions.value(key) == initial_value],
            success=[self.transactions.put(key, new_value)],
            failure=[],
        )

        return status

    @_handle_errors
    @_ensure_channel
    async def delete(self, key, prev_kv=False, return_response=False):
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
        delete_response = await self.kvstub.DeleteRange(
            delete_request,
            timeout=self.timeout,
            metadata=self.metadata
        )
        if return_response:
            return delete_response
        return delete_response.deleted >= 1

    @_handle_errors
    @_ensure_channel
    async def delete_prefix(self, prefix):
        """Delete a range of keys with a prefix in etcd."""
        delete_request = self._build_delete_request(
            prefix,
            range_end=utils.prefix_range_end(utils.to_bytes(prefix))
        )
        return await self.kvstub.DeleteRange(
            delete_request,
            timeout=self.timeout,
            metadata=self.metadata
        )

    @_handle_errors
    @_ensure_channel
    async def status(self):
        """Get the status of the responding member."""
        status_request = etcdrpc.StatusRequest()
        status_response = await self.maintenancestub.Status(
            status_request,
            timeout=self.timeout,
            metadata=self.metadata
        )

        async for m in self.members():
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
    @_ensure_channel
    async def add_watch_callback(self, *args, **kwargs):
        """
        Watch a key or range of keys and call a callback on every event.

        If timeout was declared during the client initialization and
        the watch cannot be created during that time the method raises
        a ``WatchTimedOut`` exception.

        :param key: key to watch
        :param callback: callback function

        :returns: watch_id. Later it could be used for cancelling watch.
        """
        try:
            return await self.watcher.add_callback(*args, **kwargs)
        except asyncio.QueueEmpty:
            raise exceptions.WatchTimedOut()

    @_handle_errors
    @_ensure_channel
    async def watch(self, key, **kwargs):
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
        event_queue = asyncio.Queue()
        watch_id = await self.add_watch_callback(key, event_queue.put,
                                                 **kwargs)
        canceled = asyncio.Event()

        async def cancel():
            canceled.set()
            await event_queue.put(None)
            await self.cancel_watch(watch_id)

        @_handle_errors
        async def iterator():
            while not canceled.is_set():
                event = await event_queue.get()
                if event is None:
                    canceled.set()
                if isinstance(event, Exception):
                    canceled.set()
                    raise event
                if not canceled.is_set():
                    yield event

        return iterator(), cancel

    @_handle_errors
    @_ensure_channel
    async def watch_prefix(self, key_prefix, **kwargs):
        """Watches a range of keys with a prefix."""
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))
        return await self.watch(key_prefix, **kwargs)

    @_handle_errors
    @_ensure_channel
    async def watch_once(self, key, timeout=None, **kwargs):
        """
        Watch a key and stops after the first event.

        If the timeout was specified and event didn't arrived method
        will raise ``WatchTimedOut`` exception.

        :param key: key to watch
        :param timeout: (optional) timeout in seconds.
        :returns: ``Event``
        """
        event_queue = asyncio.Queue()

        watch_id = await self.add_watch_callback(key, event_queue.put,
                                                 **kwargs)

        try:
            return await asyncio.wait_for(event_queue.get(), timeout)
        except (asyncio.QueueEmpty, asyncio.TimeoutError):
            raise exceptions.WatchTimedOut()
        finally:
            await self.cancel_watch(watch_id)

    @_handle_errors
    @_ensure_channel
    async def watch_prefix_once(self, key_prefix, timeout=None, **kwargs):
        """
        Watches a range of keys with a prefix and stops after the first event.

        If the timeout was specified and event didn't arrived method
        will raise ``WatchTimedOut`` exception.
        """
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))
        return await self.watch_once(key_prefix, timeout=timeout, **kwargs)

    @_handle_errors
    @_ensure_channel
    async def cancel_watch(self, watch_id):
        """
        Stop watching a key or range of keys.

        :param watch_id: watch_id returned by ``add_watch_callback`` method
        """
        await self.watcher.cancel(watch_id)

    @_handle_errors
    @_ensure_channel
    async def transaction(self, compare, success=None, failure=None):
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
        txn_response = await self.kvstub.Txn(
            transaction_request,
            timeout=self.timeout,
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
    @_ensure_channel
    async def lease(self, ttl, lease_id=None):
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
        lease_grant_response = await self.leasestub.LeaseGrant(
            lease_grant_request,
            timeout=self.timeout,
            metadata=self.metadata
        )
        return leases.Lease(lease_id=lease_grant_response.ID,
                            ttl=lease_grant_response.TTL,
                            etcd_client=self)

    @_handle_errors
    @_ensure_channel
    async def revoke_lease(self, lease_id):
        """
        Revoke a lease.

        :param lease_id: ID of the lease to revoke.
        """
        lease_revoke_request = etcdrpc.LeaseRevokeRequest(ID=lease_id)
        await self.leasestub.LeaseRevoke(
            lease_revoke_request,
            timeout=self.timeout,
            metadata=self.metadata
        )

    @_handle_errors
    def refresh_lease(self, lease_id):
        client = self

        async def request_stream():
            nonlocal client
            await client.open()
            yield etcdrpc.LeaseKeepAliveRequest(ID=lease_id)

        return self.leasestub.LeaseKeepAlive(
            request_stream(),
            timeout=self.timeout,
            metadata=self.metadata)

    @_handle_errors
    @_ensure_channel
    async def get_lease_info(self, lease_id, *, keys=True):
        # only available in etcd v3.1.0 and later
        ttl_request = etcdrpc.LeaseTimeToLiveRequest(ID=lease_id,
                                                     keys=keys)
        return await self.leasestub.LeaseTimeToLive(
            ttl_request,
            timeout=self.timeout,
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
    @_ensure_channel
    async def add_member(self, urls):
        """
        Add a member into the cluster.

        :returns: new member
        :rtype: :class:`.Member`
        """
        member_add_request = etcdrpc.MemberAddRequest(peerURLs=urls)

        member_add_response = await self.clusterstub.MemberAdd(
            member_add_request,
            timeout=self.timeout,
            metadata=self.metadata
        )

        member = member_add_response.member
        return etcd3.members.Member(member.ID,
                                    member.name,
                                    member.peerURLs,
                                    member.clientURLs,
                                    etcd_client=self)

    @_handle_errors
    @_ensure_channel
    async def remove_member(self, member_id):
        """
        Remove an existing member from the cluster.

        :param member_id: ID of the member to remove
        """
        member_rm_request = etcdrpc.MemberRemoveRequest(ID=member_id)
        await self.clusterstub.MemberRemove(
            member_rm_request,
            timeout=self.timeout,
            metadata=self.metadata
        )

    @_handle_errors
    @_ensure_channel
    async def update_member(self, member_id, peer_urls):
        """
        Update the configuration of an existing member in the cluster.

        :param member_id: ID of the member to update
        :param peer_urls: new list of peer urls the member will use to
                          communicate with the cluster
        """
        member_update_request = etcdrpc.MemberUpdateRequest(ID=member_id,
                                                            peerURLs=peer_urls)
        await self.clusterstub.MemberUpdate(
            member_update_request,
            timeout=self.timeout,
            metadata=self.metadata
        )

    @_ensure_channel
    async def members(self):
        """
        List of all members associated with the cluster.

        :type: sequence of :class:`.Member`

        """
        member_list_request = etcdrpc.MemberListRequest()
        member_list_response = await self.clusterstub.MemberList(
            member_list_request,
            timeout=self.timeout,
            metadata=self.metadata
        )

        for member in member_list_response.members:
            yield etcd3.members.Member(member.ID,
                                       member.name,
                                       member.peerURLs,
                                       member.clientURLs,
                                       etcd_client=self)

    @_handle_errors
    @_ensure_channel
    async def compact(self, revision, physical=False):
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
        await self.kvstub.Compact(
            compact_request,
            timeout=self.timeout,
            metadata=self.metadata
        )

    @_handle_errors
    @_ensure_channel
    async def defragment(self):
        """Defragment a member's backend database to recover storage space."""
        defrag_request = etcdrpc.DefragmentRequest()
        await self.maintenancestub.Defragment(
            defrag_request,
            timeout=self.timeout,
            metadata=self.metadata
        )

    @_handle_errors
    @_ensure_channel
    async def hash(self):
        """
        Return the hash of the local KV state.

        :returns: kv state hash
        :rtype: int
        """
        hash_request = etcdrpc.HashRequest()
        return (await self.maintenancestub.Hash(hash_request)).hash

    @_handle_errors
    @_ensure_channel
    async def create_alarm(self, member_id=0):
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
        alarm_response = await self.maintenancestub.Alarm(
            alarm_request,
            timeout=self.timeout,
            metadata=self.metadata
        )

        return [Alarm(alarm.alarm, alarm.memberID)
                for alarm in alarm_response.alarms]

    @_handle_errors
    @_ensure_channel
    async def list_alarms(self, member_id=0, alarm_type='none'):
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
        alarm_response = await self.maintenancestub.Alarm(
            alarm_request,
            timeout=self.timeout,
            metadata=self.metadata
        )

        for alarm in alarm_response.alarms:
            yield Alarm(alarm.alarm, alarm.memberID)

    @_handle_errors
    @_ensure_channel
    async def disarm_alarm(self, member_id=0):
        """Cancel an alarm.

        :param member_id: The cluster member id to cancel an alarm.
                          If 0, the alarm is canceled for all the members
                          of the cluster.
        :returns: List of :class:`.Alarm`
        """
        alarm_request = self._build_alarm_request('deactivate',
                                                  member_id,
                                                  'no space')
        alarm_response = await self.maintenancestub.Alarm(
            alarm_request,
            timeout=self.timeout,
            metadata=self.metadata
        )

        return [Alarm(alarm.alarm, alarm.memberID)
                for alarm in alarm_response.alarms]

    @_handle_errors
    @_ensure_channel
    async def snapshot(self, file_obj):
        """Take a snapshot of the database.

        :param file_obj: A file-like object to write the database contents in.
        """
        snapshot_request = etcdrpc.SnapshotRequest()
        snapshot_response = self.maintenancestub.Snapshot(
            snapshot_request,
            timeout=self.timeout,
            metadata=self.metadata
        )

        async for response in snapshot_response:
            file_obj.write(response.blob)

    @staticmethod
    def _build_get_range_request(key,
                                 range_end=None,
                                 limit=None,
                                 revision=None,
                                 sort_order=None,
                                 sort_target='key',
                                 serializable=False,
                                 keys_only=False,
                                 count_only=None,
                                 min_mod_revision=None,
                                 max_mod_revision=None,
                                 min_create_revision=None,
                                 max_create_revision=None):
        range_request = etcdrpc.RangeRequest()
        range_request.key = utils.to_bytes(key)
        range_request.keys_only = keys_only
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

        range_request.serializable = serializable

        return range_request

    @staticmethod
    def _build_put_request(key, value, lease=None, prev_kv=False):
        put_request = etcdrpc.PutRequest()
        put_request.key = utils.to_bytes(key)
        put_request.value = utils.to_bytes(value)
        put_request.lease = utils.lease_to_id(lease)
        put_request.prev_kv = prev_kv

        return put_request

    @staticmethod
    def _build_delete_request(key,
                              range_end=None,
                              prev_kv=False):
        delete_request = etcdrpc.DeleteRangeRequest()
        delete_request.key = utils.to_bytes(key)
        delete_request.prev_kv = prev_kv

        if range_end is not None:
            delete_request.range_end = utils.to_bytes(range_end)

        return delete_request

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

    @staticmethod
    def _build_alarm_request(alarm_action, member_id, alarm_type):
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
