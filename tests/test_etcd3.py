"""
test_etcd3
----------------------------------

Tests for `etcd3` module.
"""

import json
import os
import subprocess

import pytest

import etcd3


os.environ['ETCDCTL_API'] = '3'


def etcdctl(args):
    args = ['etcdctl', '-w', 'json'] + args.split()
    output = subprocess.check_output(args)
    return json.loads(output.decode('utf-8'))


class TestEtcd3(object):

    @classmethod
    def setup_class(cls):
        pass

    def test_client_stub(self):
        etcd = etcd3.client()
        assert etcd is not None

    def test_get_unknown_key(self):
        etcd = etcd3.client()
        with pytest.raises(etcd3.exceptions.KeyNotFoundError):
            etcd.get('probably-invalid-key')

    def test_get_key(self):
        etcdctl('put /doot/a_key some_value')
        etcd = etcd3.client()
        etcd.get('/doot/a_key')

    def test_put_key(self):
        etcd = etcd3.client()
        etcd.put('/doot/put_1', 'this is a doot')

    @classmethod
    def teardown_class(cls):
        etcdctl('del --prefix /doot')
