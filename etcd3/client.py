import threading

import grpc

from six.moves import queue

import etcd3.etcdrpc as etcdrpc
import etcd3.exceptions as exceptions
import etcd3.leases as leases
import etcd3.locks as locks
import etcd3.members
import etcd3.transactions as transactions
import etcd3.utils as utils
import etcd3.watch as watch


class Transactions(object):
    def __init__(self):
        self.value = transactions.Value
        self.version = transactions.Version
        self.create = transactions.Create
        self.mod = transactions.Mod

        self.put = transactions.Put
        self.get = transactions.Get
        self.delete = transactions.Delete


class Etcd3Client(object):
    def __init__(self, host='localhost', port=2379):
        self.channel = grpc.insecure_channel('{host}:{port}'.format(
            host=host, port=port)
        )
        self.kvstub = etcdrpc.KVStub(self.channel)
        self.watcher = watch.Watcher(etcdrpc.WatchStub(self.channel))
        self.clusterstub = etcdrpc.ClusterStub(self.channel)
        self.leasestub = etcdrpc.LeaseStub(self.channel)
        self.maintenancestub = etcdrpc.MaintenanceStub(self.channel)
        self.transactions = Transactions()

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

    def get(self, key):
        """
        Get the value of a key from etcd.

        :param key: key in etcd to get
        :returns: value of key
        :rtype: bytes
        """
        range_request = self._build_get_range_request(key)
        range_response = self.kvstub.Range(range_request)

        if range_response.count < 1:
            raise exceptions.KeyNotFoundError(
                'the key "{}" was not found'.format(key))
        else:
            # smells funny - there must be a cleaner way to get the value?
            return range_response.kvs.pop().value

    def get_prefix(self, key_prefix, sort_order=None, sort_target='key'):
        """
        Get a range of keys with a prefix.

        :param key_prefix: first key in range

        :returns: sequence of (key, value) tuples
        """
        range_request = self._build_get_range_request(
            key=key_prefix,
            range_end=utils.increment_last_byte(utils.to_bytes(key_prefix)),
            sort_order=sort_order,
        )

        range_response = self.kvstub.Range(range_request)

        if range_response.count < 1:
            raise exceptions.KeyNotFoundError('no keys found')
        else:
            for kv in range_response.kvs:
                yield (kv.key, kv.value)

    def get_all(self, sort_order=None, sort_target='key'):
        """
        Get all keys currently stored in etcd.

        :returns: sequence of (key, value) tuples
        """
        range_request = self._build_get_range_request(
            key=b'\0',
            range_end=b'\0',
            sort_order=sort_order,
            sort_target=sort_target,
        )

        range_response = self.kvstub.Range(range_request)

        if range_response.count < 1:
            raise exceptions.KeyNotFoundError('no keys')
        else:
            for kv in range_response.kvs:
                yield (kv.key, kv.value)

    def _build_put_request(self, key, value, lease=None):
        put_request = etcdrpc.PutRequest()
        put_request.key = utils.to_bytes(key)
        put_request.value = utils.to_bytes(value)
        put_request.lease = utils.lease_to_id(lease)
        return put_request

    def put(self, key, value, lease=None):
        """
        Save a value to etcd.

        :param key: key in etcd to set
        :param value: value to set key to
        :type value: bytes
        :param lease: Lease to associate with this key.
        :type lease: either :class:`.Lease`, or int (ID of lease)
        """
        put_request = self._build_put_request(key, value, lease=lease)
        self.kvstub.Put(put_request)

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

    def delete(self, key):
        """
        Delete a single key in etcd.

        :param key: key in etcd to delete
        """
        delete_request = self._build_delete_request(key)
        self.kvstub.DeleteRange(delete_request)

    def delete_prefix(self, prefix):
        """Delete a range of keys with a prefix in etcd."""
        delete_request = self._build_delete_request(
            prefix,
            range_end=utils.increment_last_byte(utils.to_bytes(prefix))
        )
        return self.kvstub.DeleteRange(delete_request)

    def add_watch_callback(self, *args, **kwargs):
        """
        Watch a key or range of keys and call a callback on every event.

        :param key: key to watch
        :param callback: callback function

        :returns: watch_id. Later it could be used for cancelling watch.
        """
        return self.watcher.add_callback(*args, **kwargs)

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

    def watch_prefix(self, key_prefix, **kwargs):
        """The same as ``watch``, but watches a range of keys with a prefix."""
        kwargs['range_end'] = \
            utils.increment_last_byte(utils.to_bytes(key_prefix))
        return self.watch(key_prefix, **kwargs)

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

    def watch_prefix_once(self, key_prefix, timeout=None, **kwargs):
        """
        The same as ``watch_once``, but watches a range of keys with a prefix.

        If the timeout was specified and event didn't arrived method
        will raise ``WatchTimedOut`` exception.
        """
        kwargs['range_end'] = \
            utils.increment_last_byte(utils.to_bytes(key_prefix))
        return self.watch_once(key_prefix, timeout=timeout, **kwargs)

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
        txn_response = self.kvstub.Txn(transaction_request)

        responses = []
        for response in txn_response.responses:
            response_type = response.WhichOneof('response')
            if response_type == 'response_put':
                responses.append(None)

            elif response_type == 'response_range':
                range_kvs = []
                for kv in response.response_range.kvs:
                    range_kvs.append((kv.key, kv.value))

                responses.append(range_kvs)

        return txn_response.succeeded, responses

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
        lease_grant_response = self.leasestub.LeaseGrant(lease_grant_request)
        return leases.Lease(lease_id=lease_grant_response.ID,
                            ttl=lease_grant_response.TTL,
                            etcd_client=self)

    def revoke_lease(self, lease_id):
        """
        Revoke a lease.

        :param lease_id: ID of the lease to revoke.
        """
        lease_revoke_request = etcdrpc.LeaseRevokeRequest(ID=lease_id)
        self.leasestub.LeaseRevoke(lease_revoke_request)

    def refresh_lease(self, lease_id):
        keep_alive_request = etcdrpc.LeaseKeepAliveRequest(ID=lease_id)
        request_stream = [keep_alive_request]
        for response in self.leasestub.LeaseKeepAlive(request_stream):
            yield response

    def get_lease_info(self, lease_id):
        # only available in etcd v3.1.0 and later
        ttl_request = etcdrpc.LeaseTimeToLiveRequest(ID=lease_id,
                                                     keys=True)
        return self.leasestub.LeaseTimeToLive(ttl_request)

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

    def add_member(self, urls):
        """
        Add a member into the cluster.

        :returns: new member
        :rtype: :class:`.Member`
        """
        member_add_request = etcdrpc.MemberAddRequest(peerURLs=urls)

        member_add_response = self.clusterstub.MemberAdd(member_add_request)
        member = member_add_response.member
        return etcd3.members.Member(member.ID,
                                    member.name,
                                    member.peerURLs,
                                    member.clientURLs,
                                    etcd_client=self)

    def remove_member(self, member_id):
        """
        Remove an existing member from the cluster.

        :param member_id: ID of the member to remove
        """
        member_rm_request = etcdrpc.MemberRemoveRequest(ID=member_id)
        self.clusterstub.MemberRemove(member_rm_request)

    def update_member(self, member_id, peer_urls):
        """
        Update the configuration of an existing member in the cluster.

        :param member_id: ID of the member to update
        :param peer_urls: new list of peer urls the member will use to
                          communicate with the cluster
        """
        member_update_request = etcdrpc.MemberUpdateRequest(ID=member_id,
                                                            peerURLs=peer_urls)
        self.clusterstub.MemberUpdate(member_update_request)

    @property
    def members(self):
        """
        List of all members associated with the cluster.

        :type: sequence of :class:`.Member`

        """
        member_list_request = etcdrpc.MemberListRequest()
        member_list_response = self.clusterstub.MemberList(member_list_request)

        for member in member_list_response.members:
            yield etcd3.members.Member(member.ID,
                                       member.name,
                                       member.peerURLs,
                                       member.clientURLs,
                                       etcd_client=self)

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
        self.kvstub.Compact(compact_request)

    def defragment(self):
        """Defragment a member's backend database to recover storage space."""
        defrag_request = etcdrpc.DefragmentRequest()
        self.maintenancestub.Defragment(defrag_request)


def client(host='localhost', port=2379):
    """Return an instance of an Etcd3Client."""
    return Etcd3Client(host=host, port=port)
