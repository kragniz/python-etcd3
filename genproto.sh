#!/usr/bin/env bash

set -e

sed -i -e '/gogoproto/d' src/etcd3/proto/rpc.proto
sed -i -e 's/etcd\/mvcc\/mvccpb\/kv.proto/kv.proto/g' src/etcd3/proto/rpc.proto
sed -i -e 's/etcd\/auth\/authpb\/auth.proto/auth.proto/g' src/etcd3/proto/rpc.proto
sed -i -e '/google\/api\/annotations.proto/d' src/etcd3/proto/rpc.proto
sed -i -e '/option (google.api.http)/,+3d' src/etcd3/proto/rpc.proto
python -m grpc.tools.protoc -Isrc/etcd3/proto \
    --python_out=src/etcd3/etcdrpc/ \
    --grpc_python_out=src/etcd3/etcdrpc/ \
    src/etcd3/proto/rpc.proto src/etcd3/proto/auth.proto src/etcd3/proto/kv.proto
sed -i -e 's/import auth_pb2/from etcd3.etcdrpc import auth_pb2/g' src/etcd3/etcdrpc/rpc_pb2.py
sed -i -e 's/import kv_pb2/from etcd3.etcdrpc import kv_pb2/g' src/etcd3/etcdrpc/rpc_pb2.py
sed -i -e 's/import rpc_pb2/from etcd3.etcdrpc import rpc_pb2/g' src/etcd3/etcdrpc/rpc_pb2_grpc.py
