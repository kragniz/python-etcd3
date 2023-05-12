"""Tests for `etcd3.aioclient` module."""

import asyncio
import base64
import os
import string
from unittest import mock

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


class MockedException(grpc.aio.AioRpcError):
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code

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

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    async def test_put_if_not_exists(self, etcd, string):
        txn_status = await etcd.put_if_not_exists('/doot/put_1', string)
        assert txn_status is True

        txn_status = await etcd.put_if_not_exists('/doot/put_1', string)
        assert txn_status is False

        etcdctl('del', '/doot/put_1')

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
            raise MockedException(grpc.StatusCode.UNAVAILABLE)

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
            raise MockedException(grpc.StatusCode.UNAVAILABLE)

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

    async def test_watch_responses(self, etcd):
        # Test watch_response & watch_once_response
        put_response = await etcd.put('/doot/watch', '0')
        revision = put_response.header.revision
        await etcd.put('/doot/watch', '1')
        responses_iterator, cancel = \
            await etcd.watch_response('/doot/watch', start_revision=revision)

        response_1 = await responses_iterator.__anext__()
        await cancel()
        response_2 = await etcd.watch_once_response('/doot/watch',
                                                    start_revision=revision)

        for response in [response_1, response_2]:
            count = 0
            # check that the response contains the etcd revision
            assert response.header.revision > 0
            assert len(list(response.events)) == 2
            for event in response.events:
                assert event.key == b'/doot/watch'
                assert event.value == utils.to_bytes(str(count))
                count += 1

        # Test watch_prefix_response & watch_prefix_once_response
        success_ops = [etcd.transactions.put('/doot/watch/prefix/0', '0'),
                       etcd.transactions.put('/doot/watch/prefix/1', '1')]
        txn_response = await etcd.transaction([], success_ops, [])
        revision = txn_response[1][0].response_put.header.revision

        responses_iterator, cancel = \
            await etcd.watch_prefix_response('/doot/watch/prefix/',
                                             start_revision=revision)

        response_1 = await responses_iterator.__anext__()
        await cancel()
        response_2 = await etcd.watch_prefix_once_response('/doot/watch/prefix/',
                                                           start_revision=revision)

        for response in [response_1, response_2]:
            count = 0
            assert response.header.revision == revision
            assert len(list(response.events)) == 2
            for event in response.events:
                assert event.key == \
                    utils.to_bytes('/doot/watch/prefix/{}'.format(count))
                assert event.value == utils.to_bytes(str(count))
                count += 1

    async def test_transaction_success(self, etcd):
        etcdctl('put', '/doot/txn', 'dootdoot')
        await etcd.transaction(
            compare=[etcd.transactions.value('/doot/txn') == 'dootdoot'],
            success=[etcd.transactions.put('/doot/txn', 'success')],
            failure=[etcd.transactions.put('/doot/txn', 'failure')]
        )
        out = etcdctl('get', '/doot/txn')
        assert base64.b64decode(out['kvs'][0]['value']) == b'success'

    async def test_transaction_failure(self, etcd):
        etcdctl('put', '/doot/txn', 'notdootdoot')
        await etcd.transaction(
            compare=[etcd.transactions.value('/doot/txn') == 'dootdoot'],
            success=[etcd.transactions.put('/doot/txn', 'success')],
            failure=[etcd.transactions.put('/doot/txn', 'failure')]
        )
        out = etcdctl('get', '/doot/txn')
        assert base64.b64decode(out['kvs'][0]['value']) == b'failure'

    @pytest.mark.skipif(etcd_version < 'v3.3',
                        reason="requires etcd v3.3 or higher")
    async def test_nested_transactions(self, etcd):
        await etcd.transaction(
            compare=[],
            success=[etcd.transactions.put('/doot/txn1', '1'),
                     etcd.transactions.txn(
                         compare=[],
                         success=[etcd.transactions.put('/doot/txn2', '2')],
                         failure=[])],
            failure=[]
        )
        value, _ = await etcd.get('/doot/txn1')
        assert value == b'1'
        value, _ = await etcd.get('/doot/txn2')
        assert value == b'2'

    @pytest.mark.skipif(etcd_version < 'v3.3',
                        reason="requires etcd v3.3 or higher")
    async def test_transaction_range_conditions(self, etcd):
        etcdctl('put', '/doot/key1', 'dootdoot')
        etcdctl('put', '/doot/key2', 'notdootdoot')
        range_end = utils.prefix_range_end(utils.to_bytes('/doot/'))
        compare = [etcd.transactions.value('/doot/', range_end) == 'dootdoot']
        status, _ = await etcd.transaction(compare=compare, success=[], failure=[])
        assert not status
        etcdctl('put', '/doot/key2', 'dootdoot')
        status, _ = await etcd.transaction(compare=compare, success=[], failure=[])
        assert status

    async def test_replace_success(self, etcd):
        await etcd.put('/doot/thing', 'toot')
        status = await etcd.replace('/doot/thing', 'toot', 'doot')
        v, _ = await etcd.get('/doot/thing')
        assert v == b'doot'
        assert status is True

    async def test_replace_fail(self, etcd):
        await etcd.put('/doot/thing', 'boot')
        status = await etcd.replace('/doot/thing', 'toot', 'doot')
        v, _ = await etcd.get('/doot/thing')
        assert v == b'boot'
        assert status is False

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

    async def test_get_prefix_error_handling(self, etcd):
        with pytest.raises(TypeError, match="Don't use "):
            await etcd.get_prefix('a_prefix', range_end='end')

    async def test_get_range(self, etcd):
        for char in string.ascii_lowercase:
            if char < 'p':
                etcdctl('put', '/doot/' + char, 'i am in range')
            else:
                etcdctl('put', '/doot/' + char, 'i am not in range')

        values = list(await etcd.get_range('/doot/a', '/doot/p'))
        assert len(values) == 15
        for value, _ in values:
            assert value == b'i am in range'

    async def test_all_not_found_error(self, etcd):
        result = list(await etcd.get_all())
        assert not result

    async def test_range_not_found_error(self, etcd):
        for i in range(5):
            etcdctl('put', '/doot/notrange{}'.format(i), 'i am a not range')

        result = list(await etcd.get_prefix('/doot/range'))
        assert not result

    async def test_get_all(self, etcd):
        for i in range(20):
            etcdctl('put', '/doot/range{}'.format(i), 'i am in all')

        for i in range(5):
            etcdctl('put', '/doot/notrange{}'.format(i), 'i am in all')
        values = list(await etcd.get_all())
        assert len(values) == 25
        for value, _ in values:
            assert value == b'i am in all'

    async def test_get_all_keys_only(self, etcd):
        for i in range(20):
            etcdctl('put', '/doot/range{}'.format(i), 'i am in all')

        for i in range(5):
            etcdctl('put', '/doot/notrange{}'.format(i), 'i am in all')
        values = list(await etcd.get_all(keys_only=True))
        assert len(values) == 25
        for value, meta in values:
            assert meta.key.startswith(b"/doot/")
            assert not value

    async def test_get_count_only(self, etcd):
        for i in range(20):
            etcdctl('put', '/doot/count{}'.format(i), 'i am in all')
        resp = await etcd.get_prefix_response(
            key_prefix='/doot/count',
            count_only=True
        )
        assert len(resp.kvs) == 0
        assert resp.count == 20

    async def test_get_limit(self, etcd):
        for i in range(20):
            etcdctl('put', '/doot/limit{}'.format(i), 'i am in all')
        for i in range(20):
            resp = await etcd.get_prefix_response(key_prefix='/doot/limit', limit=i)
            assert resp.count == 20
            if i == 0 or i == 20:
                assert len(resp.kvs) == 20
                assert resp.more is False
            else:
                assert len(resp.kvs) == i
                assert resp.more is True

    async def test_get_revision(self, etcd):
        revisions = []
        for i in range(20):
            resp = etcdctl('put', '/doot/revision{}'.format(i), 'i am in all')
            revisions.append(resp['header']['revision'])
        for i, revision in enumerate(revisions):
            resp = await etcd.get_prefix_response(
                key_prefix='/doot/revision',
                revision=revision
            )
            assert resp.count == min(len(revisions), i + 1)

    async def test_get_max_mod_revision(self, etcd):
        revisions = []
        for i in range(5):
            resp = etcdctl('put', '/doot/revision', str(i))
            revisions.append(resp['header']['revision'])
        for revision in revisions:
            resp = await etcd.get_response(
                key='/doot/revision',
                max_mod_revision=revision
            )
            if revision == revisions[-1]:
                assert len(resp.kvs) == 1
            else:
                assert len(resp.kvs) == 0

    async def test_sort_order(self, etcd):
        def remove_prefix(string, prefix):
            return string[len(prefix):]

        initial_keys = 'abcde'
        initial_values = 'qwert'

        for k, v in zip(initial_keys, initial_values):
            etcdctl('put', '/doot/{}'.format(k), v)

        keys = ''
        results = await etcd.get_prefix('/doot', sort_order='ascend')
        for value, meta in results:
            keys += remove_prefix(meta.key.decode('utf-8'), '/doot/')

        assert keys == initial_keys

        reverse_keys = ''
        results = await etcd.get_prefix('/doot', sort_order='descend')
        for value, meta in results:
            reverse_keys += remove_prefix(meta.key.decode('utf-8'), '/doot/')

        assert reverse_keys == ''.join(reversed(initial_keys))

    async def test_get_response(self, etcd):
        etcdctl('put', '/foo/key1', 'value1')
        etcdctl('put', '/foo/key2', 'value2')
        response = await etcd.get_response('/foo/key1')
        assert response.header.revision > 0
        assert response.count == 1
        assert response.kvs[0].key == b'/foo/key1'
        assert response.kvs[0].value == b'value1'
        response = await etcd.get_prefix_response('/foo/', sort_order='ascend')
        assert response.header.revision > 0
        assert response.count == 2
        assert response.kvs[0].key == b'/foo/key1'
        assert response.kvs[0].value == b'value1'
        assert response.kvs[1].key == b'/foo/key2'
        assert response.kvs[1].value == b'value2'
        # Test that the response header is accessible even when the
        # requested key or range of keys does not exist
        etcdctl('del', '--prefix', '/foo/')
        response = await etcd.get_response('/foo/key1')
        assert response.count == 0
        assert response.header.revision > 0
        response = await etcd.get_prefix_response('/foo/')
        assert response.count == 0
        assert response.header.revision > 0
        response = await etcd.get_range_response('/foo/key1', '/foo/key3')
        assert response.count == 0
        assert response.header.revision > 0
        response = await etcd.get_all_response()
        assert response.count == 0
        assert response.header.revision > 0

    async def test_lease_grant(self, etcd):
        lease = await etcd.lease(1)

        assert isinstance(lease.ttl, int)
        assert isinstance(lease.id, int)

    async def test_lease_revoke(self, etcd):
        lease = await etcd.lease(1)
        await lease.revoke()

    @pytest.mark.skipif(etcd_version.startswith('v3.0'),
                        reason="requires etcd v3.1 or higher")
    async def test_lease_keys_empty(self, etcd):
        lease = await etcd.lease(1)
        assert await lease.get_keys() == []


    @pytest.mark.skipif(etcd_version.startswith('v3.0'),
                        reason="requires etcd v3.1 or higher")
    async def test_lease_single_key(self, etcd):
        lease = await etcd.lease(1)
        await etcd.put('/doot/lease_test', 'this is a lease', lease=lease)
        assert await lease.get_keys() == [b'/doot/lease_test']

    @pytest.mark.skipif(etcd_version.startswith('v3.0'),
                        reason="requires etcd v3.1 or higher")
    async def test_lease_expire(self, etcd):
        key = '/doot/lease_test_expire'
        lease = await etcd.lease(1)
        await etcd.put(key, 'this is a lease', lease=lease)
        assert await lease.get_keys() == [utils.to_bytes(key)]
        v, _ = await etcd.get(key)
        assert v == b'this is a lease'

        remaining_ttl = await lease.get_remaining_ttl()
        granted_ttl = await lease.get_granted_ttl()
        assert remaining_ttl <= granted_ttl

        # wait for the lease to expire
        await asyncio.sleep(granted_ttl + 2)

        v, _ = await etcd.get(key)
        assert v is None

    async def test_member_list(self, etcd):
        members = list(await etcd.get_members())
        assert len(members) == 3
        for member in members:
            assert member.name.startswith('pifpaf')
            for peer_url in member.peer_urls:
                assert peer_url.startswith('http://')
            for client_url in member.client_urls:
                assert client_url.startswith('http://')
            assert isinstance(member.id, int) is True


@pytest.mark.skipif(not os.environ.get('ETCDCTL_ENDPOINTS'),
                    reason="Expected etcd to have been run by pifpaf")
@pytest.mark.asyncio
class TestFailoverClient(object):
    @pytest_asyncio.fixture
    async def etcd(self):
        endpoint_urls = os.environ.get('ETCDCTL_ENDPOINTS').split(',')
        timeout = 5
        endpoints = []
        for url in endpoint_urls:
            url = urlparse(url)
            endpoints.append(etcd3.AioEndpoint(host=url.hostname,
                                               port=url.port,
                                               secure=False))
        async with etcd3.MultiEndpointEtcd3AioClient(endpoints=endpoints,
                                                     timeout=timeout,
                                                     failover=True) as client:
            yield client

        @retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
        def delete_keys_definitely():
            # clean up after fixture goes out of scope
            etcdctl('del', '--prefix', '/')
            out = etcdctl('get', '--prefix', '/')
            assert 'kvs' not in out

        delete_keys_definitely()

    async def test_endpoint_offline(self, etcd):
        original_endpoint = etcd.endpoint_in_use
        assert not original_endpoint.is_failed()
        exception = MockedException(grpc.StatusCode.UNAVAILABLE)
        kv_mock = mock.PropertyMock()
        kv_mock.Range.side_effect = exception
        with mock.patch('etcd3.MultiEndpointEtcd3Client.kvstub',
                        new_callable=mock.PropertyMock) as property_mock:
            property_mock.return_value = kv_mock
            with pytest.raises(etcd3.exceptions.ConnectionFailedError):
                await etcd.get("foo")
        assert original_endpoint.is_failed()
        assert etcd.endpoint_in_use is not original_endpoint
        await etcd.get("foo")
        assert not etcd.endpoint_in_use.is_failed()

    async def test_failover_during_watch(self, etcd):
        def _handle_response(*args, **kwargs):
            raise MockedException(grpc.StatusCode.UNAVAILABLE)

        async def raise_exception():
            await asyncio.sleep(1)
            etcdctl('put', '/foo', '1')
            etcd.watcher._handle_response = _handle_response

        original_endpoint = etcd.endpoint_in_use
        assert not original_endpoint.is_failed()

        events_iterator, cancel = await etcd.watch('/foo')
        asyncio.create_task(raise_exception())

        with pytest.raises(etcd3.exceptions.ConnectionFailedError):
            await events_iterator.__anext__()

        assert original_endpoint.is_failed()
        assert etcd.endpoint_in_use is not original_endpoint
        await cancel()
        assert not etcd.endpoint_in_use.is_failed()

        events_iterator, cancel = await etcd.watch('/foo')
        await etcd.put("/foo", b"2")
        assert await events_iterator.__anext__()
        await cancel()
