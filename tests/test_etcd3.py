"""
test_etcd3
----------------------------------

Tests for `etcd3` module.
"""

import base64
import json
import os
import subprocess
from six.moves.urllib.parse import urlparse

import pytest
from hypothesis import given
from hypothesis.strategies import characters
from hypothesis.strategies import text

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

    @classmethod
    def setup_class(cls):
        pass

    # TODO workout how fixtures work...
    @property
    def client(self):
        endpoint = os.environ.get('ETCD_ENDPOINT', None)
        if endpoint:
            url = urlparse(endpoint)
            return etcd3.client(host=url.hostname, port=url.port)
        else:
            return etcd3.client()

    def test_client_stub(self):
        assert self.client is not None

    def test_get_unknown_key(self):
        with pytest.raises(etcd3.exceptions.KeyNotFoundError):
            self.client.get('probably-invalid-key')

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_get_key(self, string):
        etcdctl('put', '/doot/a_key', string)
        returned = self.client.get('/doot/a_key')
        assert returned == string.encode('utf-8')

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_get_random_key(self, string):
        etcdctl('put', '/doot/' + string, 'dootdoot')
        returned = self.client.get('/doot/' + string)
        assert returned == b'dootdoot'

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_put_key(self, string):
        self.client.put('/doot/put_1', string)
        out = etcdctl('get', '/doot/put_1')
        assert base64.b64decode(out['kvs'][0]['value']) == \
            string.encode('utf-8')

    @classmethod
    def teardown_class(cls):
        etcdctl('del', '--prefix', '/doot')
