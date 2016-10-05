"""
test_etcd3
----------------------------------

Tests for `etcd3` module.
"""

import pytest

import etcd3


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
        etcd = etcd3.client()
        etcd.get('doot')

    def test_put_key(self):
        etcd = etcd3.client()
        etcd.put('doot', 'this is a doot')

    @classmethod
    def teardown_class(cls):
        pass

