from __future__ import absolute_import

__author__ = 'Louis Taylor'
__email__ = 'louis@kragniz.eu'
__version__ = '0.1.0'

__all__ = ['Etcd3Client', 'client']

import grpc

from etcd3.etcdrpc import rpc_pb2 as etcdrpc
import etcd3.exceptions as exceptions


class Etcd3Client(object):
    def __init__(self):
        self.channel = grpc.insecure_channel('localhost:2379')
        self.kvstub = etcdrpc.KVStub(self.channel)

    def get(self, key):
        '''
        Get the value of a key from etcd.
        '''
        raise exceptions.KeyNotFoundError(
            'the key "{}" was not found'.format(key))

    def put(self, key, value):
        '''
        Save a value to etcd.
        '''
        put_request = etcdrpc.PutRequest()
        put_request.key = key.encode('utf-8')
        put_request.value = value.encode('utf-8')
        self.kvstub.Put(put_request)


def client():
    '''Return an instance of an Etcd3Client'''
    return Etcd3Client()
