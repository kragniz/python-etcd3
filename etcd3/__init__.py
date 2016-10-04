# -*- coding: utf-8 -*-

__author__ = 'Louis Taylor'
__email__ = 'louis@kragniz.eu'
__version__ = '0.1.0'

import grpc

from etcdrpc import rpc_pb2 as etcdrpc

channel = grpc.insecure_channel('localhost:2379')
stub = etcdrpc.KVStub(channel)

put_request = etcdrpc.PutRequest()
put_request.key = 'doot'
put_request.value = 'toottoot'
print(stub.Put(put_request))
