"""
test_etcd3
----------------------------------

Tests for `etcd3` module.
"""

import base64
import json
import os
import subprocess

import pytest
from hypothesis import given
from hypothesis.strategies import characters
from hypothesis.strategies import text

import etcd3


os.environ['ETCDCTL_API'] = '3'


def etcdctl(*args):
    args = ['etcdctl', '-w', 'json'] + list(args)
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

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_get_key(self, string):
        etcdctl('put', '/doot/a_key', string)
        etcd = etcd3.client()
        returned = etcd.get('/doot/a_key')
        assert returned == string.encode('utf-8')

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_get_random_key(self, string):
        etcdctl('put', '/doot/' + string, 'dootdoot')
        etcd = etcd3.client()
        returned = etcd.get('/doot/' + string)
        assert returned == b'dootdoot'

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_put_key(self, string):
        etcd = etcd3.client()
        etcd.put('/doot/put_1', string)
        out = etcdctl('get', '/doot/put_1')
        assert base64.b64decode(out['kvs'][0]['value']) == \
            string.encode('utf-8')

    def test_transaction_success(self):
        etcdctl('put', '/doot/txn', 'dootdoot')
        etcd = etcd3.client()
        etcd.transaction(
            compare=[etcd.transactions.value('/doot/txn') == 'dootdoot'],
            success=[etcd.transactions.put('/doot/txn', 'success')],
            failure=[etcd.transactions.put('/doot/txn', 'failure')]
        )
        out = etcdctl('get', '/doot/txn')
        assert base64.b64decode(out['kvs'][0]['value']) == b'success'

    def test_transaction_failure(self):
        etcdctl('put', '/doot/txn', 'notdootdoot')
        etcd = etcd3.client()
        etcd.transaction(
            compare=[etcd.transactions.value('/doot/txn') == 'dootdoot'],
            success=[etcd.transactions.put('/doot/txn', 'success')],
            failure=[etcd.transactions.put('/doot/txn', 'failure')]
        )
        out = etcdctl('get', '/doot/txn')
        assert base64.b64decode(out['kvs'][0]['value']) == b'failure'

    @classmethod
    def teardown_class(cls):
        etcdctl('del', '--prefix', '/doot')
