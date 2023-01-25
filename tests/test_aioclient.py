"""Tests for `etcd3.aioclient` module."""

import os

import grpc
import pytest
import pytest_asyncio

from hypothesis import HealthCheck, given, settings
from hypothesis.strategies import characters
from six.moves.urllib.parse import urlparse
from tenacity import retry, stop_after_attempt, wait_fixed

import etcd3
import etcd3.exceptions

from .test_etcd3 import etcdctl, _out_quorum

etcd_version = os.environ.get('TEST_ETCD_VERSION', 'v3.2.8')

os.environ['ETCDCTL_API'] = '3'


# Don't set any deadline in Hypothesis
settings.register_profile(
    "default",
    deadline=None,
    suppress_health_check=(HealthCheck.function_scoped_fixture,))
settings.load_profile("default")


@pytest.mark.asyncio
class TestEtcd3AioClient(object):

    @pytest_asyncio.fixture
    async def etcd(self):
        client_params = {}
        endpoint = os.environ.get('PYTHON_ETCD_HTTP_URL')
        timeout = 5

        if endpoint:
            url = urlparse(endpoint)
            client_params = {
                'host': url.hostname,
                'port': url.port,
                'timeout': timeout,
            }

        client = await etcd3.aioclient(**client_params)
        yield client

        @retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
        def delete_keys_definitely():
            # clean up after fixture goes out of scope
            etcdctl('del', '--prefix', '/')
            out = etcdctl('get', '--prefix', '/')
            assert 'kvs' not in out

        delete_keys_definitely()

    async def test_get_unknown_key(self, etcd):
        value, meta = await etcd.get('probably-invalid-key')
        assert value is None
        assert meta is None

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    async def test_get_key(self, etcd, string):
        etcdctl('put', '/doot/a_key', string)
        returned, _ = await etcd.get('/doot/a_key')
        assert returned == string.encode('utf-8')

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    async def test_get_random_key(self, etcd, string):
        etcdctl('put', '/doot/' + string, 'dootdoot')
        returned, _ = await etcd.get('/doot/' + string)
        assert returned == b'dootdoot'

    @given(
        characters(blacklist_categories=['Cs', 'Cc']),
        characters(blacklist_categories=['Cs', 'Cc']),
    )
    async def test_get_key_serializable(self, etcd, key, string):
        etcdctl('put', '/doot/' + key, string)
        with _out_quorum():
            returned, _ = await etcd.get('/doot/' + key, serializable=True)
        assert returned == string.encode('utf-8')

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    async def test_get_have_cluster_revision(self, etcd, string):
        etcdctl('put', '/doot/' + string, 'dootdoot')
        _, md = await etcd.get('/doot/' + string)
        assert md.response_header.revision > 0
