"""Tests for `etcd3.aioclient` module."""

import asyncio
import base64
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
import etcd3.utils as utils

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

    class MockedException(grpc.aio.AioRpcError):
        def __init__(self, code):
            self._code = code

        def code(self):
            return self._code

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

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    async def test_put_key(self, etcd, string):
        await etcd.put('/doot/put_1', string)
        out = etcdctl('get', '/doot/put_1')
        assert base64.b64decode(out['kvs'][0]['value']) == \
            string.encode('utf-8')

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    async def test_put_has_cluster_revision(self, etcd, string):
        response = await etcd.put('/doot/put_1', string)
        assert response.header.revision > 0

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    async def test_put_has_prev_kv(self,etcd,  string):
        etcdctl('put', '/doot/put_1', 'old_value')
        response = await etcd.put('/doot/put_1', string, prev_kv=True)
        assert response.prev_kv.value == b'old_value'

    async def test_delete_key(self, etcd):
        etcdctl('put', '/doot/delete_this', 'delete pls')

        v, _ = await etcd.get('/doot/delete_this')
        assert v == b'delete pls'

        deleted = await etcd.delete('/doot/delete_this')
        assert deleted is True

        deleted = await etcd.delete('/doot/delete_this')
        assert deleted is False

        deleted = await etcd.delete('/doot/not_here_dude')
        assert deleted is False

        v, _ = await etcd.get('/doot/delete_this')
        assert v is None

    async def test_delete_has_cluster_revision(self, etcd):
        response = await etcd.delete('/doot/delete_this', return_response=True)
        assert response.header.revision > 0

    async def test_delete_has_prev_kv(self, etcd):
        etcdctl('put', '/doot/delete_this', 'old_value')
        response = await etcd.delete('/doot/delete_this', prev_kv=True,
                                    return_response=True)
        assert response.prev_kvs[0].value == b'old_value'

    async def test_delete_keys_with_prefix(self, etcd):
        etcdctl('put', '/foo/1', 'bar')
        etcdctl('put', '/foo/2', 'baz')

        v, _ = await etcd.get('/foo/1')
        assert v == b'bar'

        v, _ = await etcd.get('/foo/2')
        assert v == b'baz'

        response = await etcd.delete_prefix('/foo')
        assert response.deleted == 2

        v, _ = await etcd.get('/foo/1')
        assert v is None

        v, _ = await etcd.get('/foo/2')
        assert v is None

    async def test_watch_key(self, etcd):
        def update_etcd(v):
            etcdctl('put', '/doot/watch', v)
            out = etcdctl('get', '/doot/watch')
            assert base64.b64decode(out['kvs'][0]['value']) == \
                utils.to_bytes(v)

        async def update_key():
            # sleep to make watch can get the event
            await asyncio.sleep(3)
            update_etcd('0')
            await asyncio.sleep(1)
            update_etcd('1')
            await asyncio.sleep(1)
            update_etcd('2')
            await asyncio.sleep(1)
            update_etcd('3')
            await asyncio.sleep(1)

        task = asyncio.create_task(update_key())

        change_count = 0
        events_iterator, cancel = await etcd.watch(b'/doot/watch')

        async for event in events_iterator:
            assert event.key == b'/doot/watch'
            assert event.value == \
                utils.to_bytes(str(change_count))

            # if cancel worked, we should not receive event 3
            assert event.value != utils.to_bytes('3')

            change_count += 1
            if change_count > 2:
                # if cancel not work, we will block in this for-loop forever
                await cancel()

        await task

    async def test_watch_key_with_revision_compacted(self, etcd):
        etcdctl('put', '/random', '1')  # Some data to compact

        def update_etcd(v):
            etcdctl('put', '/watchcompation', v)
            out = etcdctl('get', '/watchcompation')
            assert base64.b64decode(out['kvs'][0]['value']) == utils.to_bytes(v)

        async def update_key():
            # sleep to make watch can get the event
            await asyncio.sleep(3)
            update_etcd('0')
            await asyncio.sleep(1)
            update_etcd('1')
            await asyncio.sleep(1)
            update_etcd('2')
            await asyncio.sleep(1)
            update_etcd('3')
            await asyncio.sleep(1)

        task = asyncio.create_task(update_key())

        async def watch_compacted_revision_test(test_revision):
            events_iterator, cancel = await etcd.watch(
                b'/watchcompation', start_revision=test_revision-1)

            error_raised = False
            compacted_revision = 0
            try:
                async for event in events_iterator:
                    pass
            except Exception as err:
                error_raised = True
                assert isinstance(err, etcd3.exceptions.RevisionCompactedError)
                compacted_revision = err.compacted_revision

            assert error_raised is True
            assert compacted_revision == test_revision

            change_count = 0
            events_iterator, cancel = await etcd.watch(
                b'/watchcompation', start_revision=compacted_revision)
            async for event in events_iterator:
                assert event.key == b'/watchcompation'
                assert event.value == \
                    utils.to_bytes(str(change_count))

                # if cancel worked, we should not receive event 3
                assert event.value != utils.to_bytes('3')

                change_count += 1
                if change_count > 2:
                    # if cancel not work, we will block in this for-loop forever
                    await cancel()

        _, meta = await etcd.get('/random')
        test_revision = meta.mod_revision

        await etcd.compact(test_revision)

        await watch_compacted_revision_test(test_revision)

        await task

    async def test_watch_exception_during_watch(self, etcd):
        def _handle_response(*args, **kwargs):
            raise self.MockedException(grpc.StatusCode.UNAVAILABLE)

        async def raise_exception():
            await asyncio.sleep(1)
            etcdctl('put', '/foo', '1')
            etcd.watcher._handle_response = _handle_response

        events_iterator, _ = await etcd.watch('/foo')
        asyncio.create_task(raise_exception())

        with pytest.raises(etcd3.exceptions.ConnectionFailedError):
            async for _ in events_iterator:
                pass

    async def test_watch_exception_on_establishment(self, etcd):
        def _handle_response(*args, **kwargs):
            raise self.MockedException(grpc.StatusCode.UNAVAILABLE)

        etcd.watcher._handle_response = _handle_response

        with pytest.raises(etcd3.exceptions.ConnectionFailedError):
            events_iterator, cancel = await etcd.watch('foo')

    async def test_watch_timeout_on_establishment(self):
        foo_etcd = await etcd3.aioclient(timeout=3)

        class Watch:
            async def wait_for_connection():
                await asyncio.sleep(4)

        foo_etcd.watcher._watch_stub.Watch = Watch

        with pytest.raises(etcd3.exceptions.WatchTimedOut):
            await foo_etcd.watch('foo')

    async def test_watch_prefix(self, etcd):
        def update_etcd(v):
            etcdctl('put', '/doot/watch/prefix/' + v, v)
            out = etcdctl('get', '/doot/watch/prefix/' + v)
            assert base64.b64decode(out['kvs'][0]['value']) == \
                utils.to_bytes(v)

        async def update_key():
            # sleep to make watch can get the event
            await asyncio.sleep(3)
            update_etcd('0')
            await asyncio.sleep(1)
            update_etcd('1')
            await asyncio.sleep(1)
            update_etcd('2')
            await asyncio.sleep(1)
            update_etcd('3')
            await asyncio.sleep(1)

        task = asyncio.create_task(update_key())

        change_count = 0
        events_iterator, cancel = await etcd.watch_prefix(
            '/doot/watch/prefix/')

        async for event in events_iterator:
            assert event.key == \
                utils.to_bytes('/doot/watch/prefix/{}'.format(change_count))
            assert event.value == \
                utils.to_bytes(str(change_count))

            # if cancel worked, we should not receive event 3
            assert event.value != utils.to_bytes('3')

            change_count += 1
            if change_count > 2:
                # if cancel not work, we will block in this for-loop forever
                await cancel()

        await task

    async def test_watch_prefix_callback(self, etcd):
        def update_etcd(v):
            etcdctl('put', '/doot/watch/prefix/callback/' + v, v)
            out = etcdctl('get', '/doot/watch/prefix/callback/' + v)
            assert base64.b64decode(out['kvs'][0]['value']) == \
                utils.to_bytes(v)

        async def update_key():
            # sleep to make watch can get the event
            await asyncio.sleep(3)
            update_etcd('0')
            await asyncio.sleep(1)
            update_etcd('1')
            await asyncio.sleep(1)

        events = []

        async def callback(event):
            events.extend(event.events)

        task = asyncio.create_task(update_key())

        watch_id = await etcd.add_watch_prefix_callback(
            '/doot/watch/prefix/callback/', callback)

        await task
        await etcd.cancel_watch(watch_id)

        assert len(events) == 2
        assert events[0].key.decode() == '/doot/watch/prefix/callback/0'
        assert events[0].value.decode() == '0'
        assert events[1].key.decode() == '/doot/watch/prefix/callback/1'
        assert events[1].value.decode() == '1'

    async def test_sequential_watch_prefix_once(self, etcd):
        try:
            await etcd.watch_prefix_once('/doot/', 1)
        except etcd3.exceptions.WatchTimedOut:
            pass
        try:
            await etcd.watch_prefix_once('/doot/', 1)
        except etcd3.exceptions.WatchTimedOut:
            pass
        try:
            await etcd.watch_prefix_once('/doot/', 1)
        except etcd3.exceptions.WatchTimedOut:
            pass

    async def test_get_prefix_error_handling(self, etcd):
        with pytest.raises(TypeError, match="Don't use "):
            await etcd.get_prefix('a_prefix', range_end='end')

    async def test_get_prefix(self, etcd):
        for i in range(20):
            etcdctl('put', '/doot/range{}'.format(i), 'i am a range')

        for i in range(5):
            etcdctl('put', '/doot/notrange{}'.format(i), 'i am a not range')

        values = list(await etcd.get_prefix('/doot/range'))
        assert len(values) == 20
        for value, _ in values:
            assert value == b'i am a range'

    async def test_get_prefix_keys_only(self, etcd):
        for i in range(20):
            etcdctl('put', '/doot/range{}'.format(i), 'i am a range')

        for i in range(5):
            etcdctl('put', '/doot/notrange{}'.format(i), 'i am a not range')

        values = list(await etcd.get_prefix('/doot/range', keys_only=True))
        assert len(values) == 20
        for value, meta in values:
            assert meta.key.startswith(b"/doot/range")
            assert not value

    async def test_get_prefix_serializable(self, etcd):
        for i in range(20):
            etcdctl('put', '/doot/range{}'.format(i), 'i am a range')

        with _out_quorum():
            values = list(await etcd.get_prefix(
                '/doot/range', keys_only=True, serializable=True))

        assert len(values) == 20
