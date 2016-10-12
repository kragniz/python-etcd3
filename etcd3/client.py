import grpc

from etcd3.etcdrpc import rpc_pb2 as etcdrpc
import etcd3.exceptions as exceptions
import etcd3.transactions as transactions


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
        self.transactions = Transactions()

    def get(self, key):
        '''
        Get the value of a key from etcd.

        :param key: key in etcd to get
        :returns: value of key
        :rtype: bytes
        '''
        range_request = etcdrpc.RangeRequest()
        range_request.key = key.encode('utf-8')
        range_response = self.kvstub.Range(range_request)

        if range_response.count < 1:
            raise exceptions.KeyNotFoundError(
                'the key "{}" was not found'.format(key))
        else:
            # smells funny - there must be a cleaner way to get the value?
            return range_response.kvs.pop().value

    def get_range(self, start_key, range_end='\0',
                  sort_order=None, sort_target='key'):
        '''
        Get a range of keys.

        :param start_key: first key in range
        :param range_end: upper bound of requested range

        :returns: sequence of (key, value) tuples
        '''
        range_request = etcdrpc.RangeRequest()
        range_request.key = start_key.encode('utf-8')
        range_request.range_end = range_end.encode('utf-8')

        if sort_order is None:
            range_request.sort_order = etcdrpc.RangeRequest.NONE
        elif sort_order == 'ascend':
            range_request.sort_order = etcdrpc.RangeRequest.ASCEND
        elif sort_order == 'descend':
            range_request.sort_order = etcdrpc.RangeRequest.DESCEND
        else:
            raise Exception('unknown sort order: "{}"'.format(sort_order))

        range_response = self.kvstub.Range(range_request)

        if range_response.count < 1:
            raise exceptions.KeyNotFoundError('no keys found')
        else:
            for kv in range_response.kvs:
                yield (kv.key, kv.value)

    def _build_put_request(self, key, value):
        put_request = etcdrpc.PutRequest()
        put_request.key = key.encode('utf-8')
        put_request.value = value.encode('utf-8')
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

    def delete(self, key):
        '''
        Delete a single key in etcd.

        :param key: key in etcd to delete
        '''
        delete_request = etcdrpc.DeleteRangeRequest()
        delete_request.key = key.encode('utf-8')
        self.kvstub.DeleteRange(delete_request)

    def compact(self):
        '''
        Compact the event history in etcd.
        '''
        pass

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
        self.kvstub.Txn(transaction_request)

    def add_member(self, urls):
        '''
        Add a member into the cluster.
        '''
        pass

    def remove_member(self, member_id):
        '''
        Remove an existing member from the cluster.
        '''
        pass

    def update_member(self, member_id, urls):
        '''
        Update the configuration of an existing member in the cluster.
        '''
        pass

    @property
    def members(self):
        '''
        List of all members associated with the cluster.
        '''
        pass


def client(host='localhost', port=2379):
    '''Return an instance of an Etcd3Client'''
    return Etcd3Client(host=host, port=port)
