import grpc

import etcd3.exceptions as exceptions
import etcd3.members
import etcd3.transactions as transactions
import etcd3.utils as utils
from etcd3.etcdrpc import rpc_pb2 as etcdrpc


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
        self.clusterstub = etcdrpc.ClusterStub(self.channel)
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
        '''
        Get the value of a key from etcd.

        :param key: key in etcd to get
        :returns: value of key
        :rtype: bytes
        '''
        range_request = self._build_get_range_request(key)
        range_response = self.kvstub.Range(range_request)

        if range_response.count < 1:
            raise exceptions.KeyNotFoundError(
                'the key "{}" was not found'.format(key))
        else:
            # smells funny - there must be a cleaner way to get the value?
            return range_response.kvs.pop().value

    def get_prefix(self, key_prefix, sort_order=None, sort_target='key'):
        '''
        Get a range of keys with a prefix.

        :param key_prefix: first key in range

        :returns: sequence of (key, value) tuples
        '''
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
        '''
        Get all keys currently stored in etcd.

        :returns: sequence of (key, value) tuples
        '''
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

    def _build_put_request(self, key, value):
        put_request = etcdrpc.PutRequest()
        put_request.key = utils.to_bytes(key)
        put_request.value = utils.to_bytes(value)
        return put_request

    def put(self, key, value):
        '''
        Save a value to etcd.

        :param key: key in etcd to set
        :param value: value to set key to
        :type value: bytes
        '''
        put_request = self._build_put_request(key, value)
        self.kvstub.Put(put_request)

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
        '''
        Delete a single key in etcd.

        :param key: key in etcd to delete
        '''
        delete_request = self._build_delete_request(key)
        self.kvstub.DeleteRange(delete_request)

    def delete_prefix(self, prefix):
        '''
        Delete a range of keys with a prefix in etcd.
        '''
        delete_request = self._build_delete_request(
            prefix,
            range_end=utils.increment_last_byte(prefix)
        )
        return self.kvstub.DeleteRange(delete_request)

    def compact(self, revision, physical=False):
        '''
        Compact the event history in etcd.
        '''
        compact_request = etcdrpc.CompactionRequest(revision=revision,
                                                    physical=physical)
        self.kvstub.Compact(compact_request)

    def _ops_to_requests(self, ops):
        '''
        Return a list of grpc requests from an input list of
        etcd3.transactions.{Put, Get, Delete} objects.
        '''
        request_ops = []
        for op in ops:
            if isinstance(op, transactions.Put):
                request = self._build_put_request(op.key, op.value)
                request_op = etcdrpc.RequestOp(request_put=request)
                request_ops.append(request_op)

            elif isinstance(op, transactions.Get):
                request = self._build_get_range_request(op.key)
                request_op = etcdrpc.RequestOp(request_range=request)
                request_ops.append(request_op)
            else:
                raise Exception(
                    'Unknown request class {}'.format(op.__class__))
        return request_ops

    def transaction(self, compare, success=None, failure=None):
        '''
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
        '''

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

    def add_member(self, urls):
        '''
        Add a member into the cluster.

        :returns: new member
        :rtype: :class:`.Member`
        '''
        member_add_request = etcdrpc.MemberAddRequest(peerURLs=urls)

        member_add_response = self.clusterstub.MemberAdd(member_add_request)
        member = member_add_response.member
        return etcd3.members.Member(member.ID,
                                    member.name,
                                    member.peerURLs,
                                    member.clientURLs,
                                    etcd_client=self)

    def remove_member(self, member_id):
        '''
        Remove an existing member from the cluster.

        :param member_id: ID of the member to remove
        '''
        member_rm_request = etcdrpc.MemberRemoveRequest(ID=member_id)
        self.clusterstub.MemberRemove(member_rm_request)

    def update_member(self, member_id, peer_urls):
        '''
        Update the configuration of an existing member in the cluster.

        :param member_id: ID of the member to update
        :param peer_urls: new list of peer urls the member will use to
                          communicate with the cluster
        '''
        member_update_request = etcdrpc.MemberUpdateRequest(ID=member_id,
                                                            peerURLs=peer_urls)
        self.clusterstub.MemberUpdate(member_update_request)

    @property
    def members(self):
        '''
        List of all members associated with the cluster.

        :type: sequence of :class:`.Member`

        '''
        member_list_request = etcdrpc.MemberListRequest()
        member_list_response = self.clusterstub.MemberList(member_list_request)

        for member in member_list_response.members:
            yield etcd3.members.Member(member.ID,
                                       member.name,
                                       member.peerURLs,
                                       member.clientURLs,
                                       etcd_client=self)


def client(host='localhost', port=2379):
    '''Return an instance of an Etcd3Client'''
    return Etcd3Client(host=host, port=port)
