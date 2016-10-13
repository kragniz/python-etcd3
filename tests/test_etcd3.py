"""
test_etcd3
----------------------------------

Tests for `etcd3` module.
"""

import base64
import json
import os
import subprocess

from hypothesis import given
from hypothesis.strategies import characters
import pytest
from six.moves.urllib.parse import urlparse

import etcd3


os.environ['ETCDCTL_API'] = '3'


def etcdctl(*args):
    endpoint = os.environ.get('ETCD_ENDPOINT', None)
    if endpoint:
        args = ['--endpoints', endpoint] + list(args)
    args = ['etcdctl', '-w', 'json'] + list(args)
    print(" ".join(args))
    output = subprocess.check_output(args)
    return json.loads(output.decode('utf-8'))


class TestEtcd3(object):

    @pytest.fixture
    def etcd(self):
        endpoint = os.environ.get('ETCD_ENDPOINT', None)
        if endpoint:
            url = urlparse(endpoint)
            yield etcd3.client(host=url.hostname, port=url.port)
        else:
            yield etcd3.client()

        # clean up after fixture goes out of scope
        etcdctl('del', '--prefix', '/')

    def test_get_unknown_key(self, etcd):
        with pytest.raises(etcd3.exceptions.KeyNotFoundError):
            etcd.get('probably-invalid-key')

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_get_key(self, etcd, string):
        etcdctl('put', '/doot/a_key', string)
        returned = etcd.get('/doot/a_key')
        assert returned == string.encode('utf-8')

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_get_random_key(self, etcd, string):
        etcdctl('put', '/doot/' + string, 'dootdoot')
        returned = etcd.get('/doot/' + string)
        assert returned == b'dootdoot'

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_put_key(self, etcd, string):
        etcd.put('/doot/put_1', string)
        out = etcdctl('get', '/doot/put_1')
        assert base64.b64decode(out['kvs'][0]['value']) == \
            string.encode('utf-8')

    def test_delete_key(self, etcd):
        etcdctl('put', '/doot/delete_this', 'delete pls')

        assert etcd.get('/doot/delete_this') == b'delete pls'

        etcd.delete('/doot/delete_this')

        with pytest.raises(etcd3.exceptions.KeyNotFoundError):
            etcd.get('/doot/delete_this')

    def test_transaction_success(self, etcd):
        etcdctl('put', '/doot/txn', 'dootdoot')
        etcd.transaction(
            compare=[etcd.transactions.value('/doot/txn') == 'dootdoot'],
            success=[etcd.transactions.put('/doot/txn', 'success')],
            failure=[etcd.transactions.put('/doot/txn', 'failure')]
        )
        out = etcdctl('get', '/doot/txn')
        assert base64.b64decode(out['kvs'][0]['value']) == b'success'

    def test_transaction_failure(self, etcd):
        etcdctl('put', '/doot/txn', 'notdootdoot')
        etcd.transaction(
            compare=[etcd.transactions.value('/doot/txn') == 'dootdoot'],
            success=[etcd.transactions.put('/doot/txn', 'success')],
            failure=[etcd.transactions.put('/doot/txn', 'failure')]
        )
        out = etcdctl('get', '/doot/txn')
        assert base64.b64decode(out['kvs'][0]['value']) == b'failure'

    def test_get_prefix(self, etcd):
        for i in range(20):
            etcdctl('put', '/doot/range{}'.format(i), 'i am a range')

        for i in range(5):
            etcdctl('put', '/doot/notrange{}'.format(i), 'i am a not range')

        values = list(etcd.get_prefix('/doot/range'))
        assert len(values) == 20
        for key, value in values:
            assert value == b'i am a range'


class TestUtils(object):
    def test_increment_last_byte(self):
        assert etcd3.utils.increment_last_byte(b'foo') == b'fop'
