"""
Tests for `etcd3` module.

----------------------------------
"""

import base64
import contextlib
import json
import os
import signal
import string
import subprocess
import tempfile
import threading
import time

import grpc

from hypothesis import given, settings
from hypothesis.strategies import characters

import mock

import pytest

import six
from six.moves.urllib.parse import urlparse

from tenacity import retry, stop_after_attempt, wait_fixed

import etcd3
import etcd3.etcdrpc as etcdrpc
import etcd3.exceptions
import etcd3.utils as utils
from etcd3.client import EtcdTokenCallCredentials

etcd_version = os.environ.get('TEST_ETCD_VERSION', 'v3.2.8')

os.environ['ETCDCTL_API'] = '3'

if six.PY2:
    int_types = (int, long)
else:
    int_types = (int,)


# Don't set any deadline in Hypothesis
settings.register_profile("default", deadline=None)
settings.load_profile("default")


def etcdctl(*args):
    endpoint = os.environ.get('PYTHON_ETCD_HTTP_URL')
    if endpoint:
        args = ['--endpoints', endpoint] + list(args)
    args = ['etcdctl', '-w', 'json'] + list(args)
    print(" ".join(args))
    output = subprocess.check_output(args)
    return json.loads(output.decode('utf-8'))


# def etcdctl2(*args):
#     # endpoint = os.environ.get('PYTHON_ETCD_HTTP_URL')
#     # if endpoint:
#     #     args = ['--endpoints', endpoint] + list(args)
#     # args = ['echo', 'pwd', '|', 'etcdctl', '-w', 'json'] + list(args)
#     # print(" ".join(args))
#     output = subprocess.check_output("echo pwd | ./etcdctl user add root")
#     return json.loads(output.decode('utf-8'))


@contextlib.contextmanager
def _out_quorum():
    pids = subprocess.check_output(['pgrep', '-f', '--', '--name pifpaf[12]'])
    pids = [int(pid.strip()) for pid in pids.splitlines()]
    try:
        for pid in pids:
            os.kill(pid, signal.SIGSTOP)
        yield
    finally:
        for pid in pids:
            os.kill(pid, signal.SIGCONT)


class TestEtcd3(object):

    class MockedException(grpc.RpcError):
        def __init__(self, code):
            self._code = code

        def code(self):
            return self._code

    @pytest.fixture
    def etcd(self):
        endpoint = os.environ.get('PYTHON_ETCD_HTTP_URL')
        timeout = 5
        if endpoint:
            url = urlparse(endpoint)
            with etcd3.client(host=url.hostname,
                              port=url.port,
                              timeout=timeout) as client:
                yield client
        else:
            with etcd3.client() as client:
                yield client

        @retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
        def delete_keys_definitely():
            # clean up after fixture goes out of scope
            etcdctl('del', '--prefix', '/')
            out = etcdctl('get', '--prefix', '/')
            assert 'kvs' not in out

        delete_keys_definitely()

    def test_get_unknown_key(self, etcd):
        value, meta = etcd.get('probably-invalid-key')
        assert value is None
        assert meta is None

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_get_key(self, etcd, string):
        etcdctl('put', '/doot/a_key', string)
        returned, _ = etcd.get('/doot/a_key')
        assert returned == string.encode('utf-8')

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_get_random_key(self, etcd, string):
        etcdctl('put', '/doot/' + string, 'dootdoot')
        returned, _ = etcd.get('/doot/' + string)
        assert returned == b'dootdoot'

    @given(
        characters(blacklist_categories=['Cs', 'Cc']),
        characters(blacklist_categories=['Cs', 'Cc']),
    )
    def test_get_key_serializable(self, etcd, key, string):
        etcdctl('put', '/doot/' + key, string)
        with _out_quorum():
            returned, _ = etcd.get('/doot/' + key, serializable=True)
        assert returned == string.encode('utf-8')

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_get_have_cluster_revision(self, etcd, string):
        etcdctl('put', '/doot/' + string, 'dootdoot')
        _, md = etcd.get('/doot/' + string)
        assert md.response_header.revision > 0

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_put_key(self, etcd, string):
        etcd.put('/doot/put_1', string)
        out = etcdctl('get', '/doot/put_1')
        assert base64.b64decode(out['kvs'][0]['value']) == \
            string.encode('utf-8')

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_put_has_cluster_revision(self, etcd, string):
        response = etcd.put('/doot/put_1', string)
        assert response.header.revision > 0

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_put_has_prev_kv(self, etcd, string):
        etcdctl('put', '/doot/put_1', 'old_value')
        response = etcd.put('/doot/put_1', string, prev_kv=True)
        assert response.prev_kv.value == b'old_value'

    def test_delete_key(self, etcd):
        etcdctl('put', '/doot/delete_this', 'delete pls')

        v, _ = etcd.get('/doot/delete_this')
        assert v == b'delete pls'

        deleted = etcd.delete('/doot/delete_this')
        assert deleted is True

        deleted = etcd.delete('/doot/delete_this')
        assert deleted is False

        deleted = etcd.delete('/doot/not_here_dude')
        assert deleted is False

        v, _ = etcd.get('/doot/delete_this')
        assert v is None

    def test_delete_has_cluster_revision(self, etcd):
        response = etcd.delete('/doot/delete_this', return_response=True)
        assert response.header.revision > 0

    def test_delete_has_prev_kv(self, etcd):
        etcdctl('put', '/doot/delete_this', 'old_value')
        response = etcd.delete('/doot/delete_this', prev_kv=True,
                               return_response=True)
        assert response.prev_kvs[0].value == b'old_value'

    def test_delete_keys_with_prefix(self, etcd):
        etcdctl('put', '/foo/1', 'bar')
        etcdctl('put', '/foo/2', 'baz')

        v, _ = etcd.get('/foo/1')
        assert v == b'bar'

        v, _ = etcd.get('/foo/2')
        assert v == b'baz'

        response = etcd.delete_prefix('/foo')
        assert response.deleted == 2

        v, _ = etcd.get('/foo/1')
        assert v is None

        v, _ = etcd.get('/foo/2')
        assert v is None

    def test_new_watch_error(self, etcd):
        # Trigger a failure while waiting on the new watch condition
        with mock.patch.object(etcd.watcher._new_watch_cond, 'wait',
                               side_effect=ValueError):
            with pytest.raises(ValueError):
                etcd.watch('/foo')

        # Ensure a new watch can be created
        events, cancel = etcd.watch('/foo')
        etcdctl('put', '/foo', '42')
        next(events)
        cancel()

    def test_watch_key(self, etcd):
        def update_etcd(v):
            etcdctl('put', '/doot/watch', v)
            out = etcdctl('get', '/doot/watch')
            assert base64.b64decode(out['kvs'][0]['value']) == \
                utils.to_bytes(v)

        def update_key():
            # sleep to make watch can get the event
            time.sleep(3)
            update_etcd('0')
            time.sleep(1)
            update_etcd('1')
            time.sleep(1)
            update_etcd('2')
            time.sleep(1)
            update_etcd('3')
            time.sleep(1)

        t = threading.Thread(name="update_key", target=update_key)
        t.start()

        change_count = 0
        events_iterator, cancel = etcd.watch(b'/doot/watch')
        for event in events_iterator:
            assert event.key == b'/doot/watch'
            assert event.value == \
                utils.to_bytes(str(change_count))

            # if cancel worked, we should not receive event 3
            assert event.value != utils.to_bytes('3')

            change_count += 1
            if change_count > 2:
                # if cancel not work, we will block in this for-loop forever
                cancel()

        t.join()

    def test_watch_key_with_revision_compacted(self, etcd):
        etcdctl('put', '/random', '1')  # Some data to compact

        def update_etcd(v):
            etcdctl('put', '/watchcompation', v)
            out = etcdctl('get', '/watchcompation')
            assert base64.b64decode(out['kvs'][0]['value']) == \
                utils.to_bytes(v)

        def update_key():
            # sleep to make watch can get the event
            time.sleep(3)
            update_etcd('0')
            time.sleep(1)
            update_etcd('1')
            time.sleep(1)
            update_etcd('2')
            time.sleep(1)
            update_etcd('3')
            time.sleep(1)

        t = threading.Thread(name="update_key", target=update_key)
        t.start()

        def watch_compacted_revision_test():
            events_iterator, cancel = etcd.watch(
                b'/watchcompation', start_revision=1)

            error_raised = False
            compacted_revision = 0
            try:
                next(events_iterator)
            except Exception as err:
                error_raised = True
                assert isinstance(err, etcd3.exceptions.RevisionCompactedError)
                compacted_revision = err.compacted_revision

            assert error_raised is True
            assert compacted_revision == 2

            change_count = 0
            events_iterator, cancel = etcd.watch(
                b'/watchcompation', start_revision=compacted_revision)
            for event in events_iterator:
                assert event.key == b'/watchcompation'
                assert event.value == \
                    utils.to_bytes(str(change_count))

                # if cancel worked, we should not receive event 3
                assert event.value != utils.to_bytes('3')

                change_count += 1
                if change_count > 2:
                    cancel()

        # Compact etcd and test watcher
        etcd.compact(2)

        watch_compacted_revision_test()

        t.join()

    def test_watch_exception_during_watch(self, etcd):
        def pass_exception_to_callback(callback):
            time.sleep(1)
            callback(self.MockedException(grpc.StatusCode.UNAVAILABLE))

        def add_callback_mock(*args, **kwargs):
            callback = args[1]
            t = threading.Thread(name="pass_exception_to_callback",
                                 target=pass_exception_to_callback,
                                 args=[callback])
            t.start()
            return 1

        watcher_mock = mock.MagicMock()
        watcher_mock.add_callback = add_callback_mock
        etcd.watcher = watcher_mock

        events_iterator, cancel = etcd.watch('foo')

        with pytest.raises(etcd3.exceptions.ConnectionFailedError):
            for _ in events_iterator:
                pass

    def test_watch_timeout_on_establishment(self, etcd):
        foo_etcd = etcd3.client(timeout=3)

        def slow_watch_mock(*args, **kwargs):
            time.sleep(4)

        foo_etcd.watcher._watch_stub.Watch = slow_watch_mock  # noqa

        with pytest.raises(etcd3.exceptions.WatchTimedOut):
            foo_etcd.watch('foo')

    def test_watch_prefix(self, etcd):
        def update_etcd(v):
            etcdctl('put', '/doot/watch/prefix/' + v, v)
            out = etcdctl('get', '/doot/watch/prefix/' + v)
            assert base64.b64decode(out['kvs'][0]['value']) == \
                utils.to_bytes(v)

        def update_key():
            # sleep to make watch can get the event
            time.sleep(3)
            update_etcd('0')
            time.sleep(1)
            update_etcd('1')
            time.sleep(1)
            update_etcd('2')
            time.sleep(1)
            update_etcd('3')
            time.sleep(1)

        t = threading.Thread(name="update_key_prefix", target=update_key)
        t.start()

        change_count = 0
        events_iterator, cancel = etcd.watch_prefix('/doot/watch/prefix/')
        for event in events_iterator:
            assert event.key == \
                utils.to_bytes('/doot/watch/prefix/{}'.format(change_count))
            assert event.value == \
                utils.to_bytes(str(change_count))

            # if cancel worked, we should not receive event 3
            assert event.value != utils.to_bytes('3')

            change_count += 1
            if change_count > 2:
                # if cancel not work, we will block in this for-loop forever
                cancel()

        t.join()

    def test_sequential_watch_prefix_once(self, etcd):
        try:
            etcd.watch_prefix_once('/doot/', 1)
        except etcd3.exceptions.WatchTimedOut:
            pass
        try:
            etcd.watch_prefix_once('/doot/', 1)
        except etcd3.exceptions.WatchTimedOut:
            pass
        try:
            etcd.watch_prefix_once('/doot/', 1)
        except etcd3.exceptions.WatchTimedOut:
            pass

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

    def test_ops_to_requests(self, etcd):
        with pytest.raises(Exception):
            etcd._ops_to_requests(['not_transaction_type'])
        with pytest.raises(TypeError):
            etcd._ops_to_requests(0)

    @pytest.mark.skipif(etcd_version < 'v3.3',
                        reason="requires etcd v3.3 or higher")
    def test_nested_transactions(self, etcd):
        etcd.transaction(
            compare=[],
            success=[etcd.transactions.put('/doot/txn1', '1'),
                     etcd.transactions.txn(
                         compare=[],
                         success=[etcd.transactions.put('/doot/txn2', '2')],
                         failure=[])],
            failure=[]
        )
        value, _ = etcd.get('/doot/txn1')
        assert value == b'1'
        value, _ = etcd.get('/doot/txn2')
        assert value == b'2'

    @pytest.mark.skipif(etcd_version < 'v3.3',
                        reason="requires etcd v3.3 or higher")
    def test_transaction_range_conditions(self, etcd):
        etcdctl('put', '/doot/key1', 'dootdoot')
        etcdctl('put', '/doot/key2', 'notdootdoot')
        range_end = utils.increment_last_byte(utils.to_bytes('/doot/'))
        compare = [etcd.transactions.value('/doot/', range_end) == 'dootdoot']
        status, _ = etcd.transaction(compare=compare, success=[], failure=[])
        assert not status
        etcdctl('put', '/doot/key2', 'dootdoot')
        status, _ = etcd.transaction(compare=compare, success=[], failure=[])
        assert status

    def test_replace_success(self, etcd):
        etcd.put('/doot/thing', 'toot')
        status = etcd.replace('/doot/thing', 'toot', 'doot')
        v, _ = etcd.get('/doot/thing')
        assert v == b'doot'
        assert status is True

    def test_replace_fail(self, etcd):
        etcd.put('/doot/thing', 'boot')
        status = etcd.replace('/doot/thing', 'toot', 'doot')
        v, _ = etcd.get('/doot/thing')
        assert v == b'boot'
        assert status is False

    def test_get_prefix(self, etcd):
        for i in range(20):
            etcdctl('put', '/doot/range{}'.format(i), 'i am a range')

        for i in range(5):
            etcdctl('put', '/doot/notrange{}'.format(i), 'i am a not range')

        values = list(etcd.get_prefix('/doot/range'))
        assert len(values) == 20
        for value, _ in values:
            assert value == b'i am a range'

    def test_get_prefix_keys_only(self, etcd):
        for i in range(20):
            etcdctl('put', '/doot/range{}'.format(i), 'i am a range')

        for i in range(5):
            etcdctl('put', '/doot/notrange{}'.format(i), 'i am a not range')

        values = list(etcd.get_prefix('/doot/range', keys_only=True))
        assert len(values) == 20
        for value, meta in values:
            assert meta.key.startswith(b"/doot/range")
            assert not value

    def test_get_range(self, etcd):
        for char in string.ascii_lowercase:
            if char < 'p':
                etcdctl('put', '/doot/' + char, 'i am in range')
            else:
                etcdctl('put', '/doot/' + char, 'i am not in range')

        values = list(etcd.get_range('/doot/a', '/doot/p'))
        assert len(values) == 15
        for value, _ in values:
            assert value == b'i am in range'

    def test_all_not_found_error(self, etcd):
        result = list(etcd.get_all())
        assert not result

    def test_range_not_found_error(self, etcd):
        for i in range(5):
            etcdctl('put', '/doot/notrange{}'.format(i), 'i am a not range')

        result = list(etcd.get_prefix('/doot/range'))
        assert not result

    def test_get_all(self, etcd):
        for i in range(20):
            etcdctl('put', '/doot/range{}'.format(i), 'i am in all')

        for i in range(5):
            etcdctl('put', '/doot/notrange{}'.format(i), 'i am in all')
        values = list(etcd.get_all())
        assert len(values) == 25
        for value, _ in values:
            assert value == b'i am in all'

    def test_get_all_keys_only(self, etcd):
        for i in range(20):
            etcdctl('put', '/doot/range{}'.format(i), 'i am in all')

        for i in range(5):
            etcdctl('put', '/doot/notrange{}'.format(i), 'i am in all')
        values = list(etcd.get_all(keys_only=True))
        assert len(values) == 25
        for value, meta in values:
            assert meta.key.startswith(b"/doot/")
            assert not value

    def test_sort_order(self, etcd):
        def remove_prefix(string, prefix):
            return string[len(prefix):]

        initial_keys = 'abcde'
        initial_values = 'qwert'

        for k, v in zip(initial_keys, initial_values):
            etcdctl('put', '/doot/{}'.format(k), v)

        keys = ''
        for value, meta in etcd.get_prefix('/doot', sort_order='ascend'):
            keys += remove_prefix(meta.key.decode('utf-8'), '/doot/')

        assert keys == initial_keys

        reverse_keys = ''
        for value, meta in etcd.get_prefix('/doot', sort_order='descend'):
            reverse_keys += remove_prefix(meta.key.decode('utf-8'), '/doot/')

        assert reverse_keys == ''.join(reversed(initial_keys))

    def test_get_response(self, etcd):
        etcdctl('put', '/foo/key1', 'value1')
        etcdctl('put', '/foo/key2', 'value2')
        response = etcd.get_response('/foo/key1')
        assert response.header.revision > 0
        assert response.count == 1
        assert response.kvs[0].key == b'/foo/key1'
        assert response.kvs[0].value == b'value1'
        response = etcd.get_prefix_response('/foo/', sort_order='ascend')
        assert response.header.revision > 0
        assert response.count == 2
        assert response.kvs[0].key == b'/foo/key1'
        assert response.kvs[0].value == b'value1'
        assert response.kvs[1].key == b'/foo/key2'
        assert response.kvs[1].value == b'value2'
        # Test that the response header is accessible even when the
        # requested key or range of keys does not exist
        etcdctl('del', '--prefix', '/foo/')
        response = etcd.get_response('/foo/key1')
        assert response.count == 0
        assert response.header.revision > 0
        response = etcd.get_prefix_response('/foo/')
        assert response.count == 0
        assert response.header.revision > 0
        response = etcd.get_range_response('/foo/key1', '/foo/key3')
        assert response.count == 0
        assert response.header.revision > 0
        response = etcd.get_all_response()
        assert response.count == 0
        assert response.header.revision > 0

    def test_lease_grant(self, etcd):
        lease = etcd.lease(1)

        assert isinstance(lease.ttl, int_types)
        assert isinstance(lease.id, int_types)

    def test_lease_revoke(self, etcd):
        lease = etcd.lease(1)
        lease.revoke()

    @pytest.mark.skipif(etcd_version.startswith('v3.0'),
                        reason="requires etcd v3.1 or higher")
    def test_lease_keys_empty(self, etcd):
        lease = etcd.lease(1)
        assert lease.keys == []

    @pytest.mark.skipif(etcd_version.startswith('v3.0'),
                        reason="requires etcd v3.1 or higher")
    def test_lease_single_key(self, etcd):
        lease = etcd.lease(1)
        etcd.put('/doot/lease_test', 'this is a lease', lease=lease)
        assert lease.keys == [b'/doot/lease_test']

    @pytest.mark.skipif(etcd_version.startswith('v3.0'),
                        reason="requires etcd v3.1 or higher")
    def test_lease_expire(self, etcd):
        key = '/doot/lease_test_expire'
        lease = etcd.lease(1)
        etcd.put(key, 'this is a lease', lease=lease)
        assert lease.keys == [utils.to_bytes(key)]
        v, _ = etcd.get(key)
        assert v == b'this is a lease'
        assert lease.remaining_ttl <= lease.granted_ttl

        # wait for the lease to expire
        time.sleep(lease.granted_ttl + 2)
        v, _ = etcd.get(key)
        assert v is None

    def test_member_list(self, etcd):
        assert len(list(etcd.members)) == 3
        for member in etcd.members:
            assert member.name.startswith('pifpaf')
            for peer_url in member.peer_urls:
                assert peer_url.startswith('http://')
            for client_url in member.client_urls:
                assert client_url.startswith('http://')
            assert isinstance(member.id, int_types) is True

    def test_lock_acquire(self, etcd):
        lock = etcd.lock('lock-1', ttl=10)
        assert lock.acquire() is True
        assert etcd.get(lock.key)[0] is not None
        assert lock.acquire(timeout=0) is False
        assert lock.acquire(timeout=1) is False

    def test_lock_release(self, etcd):
        lock = etcd.lock('lock-2', ttl=10)
        assert lock.acquire() is True
        assert etcd.get(lock.key)[0] is not None
        assert lock.release() is True
        v, _ = etcd.get(lock.key)
        assert v is None
        assert lock.acquire() is True
        assert lock.release() is True
        assert lock.acquire(timeout=None) is True

    def test_lock_expire(self, etcd):
        lock = etcd.lock('lock-3', ttl=3)
        assert lock.acquire() is True
        assert etcd.get(lock.key)[0] is not None
        # wait for the lease to expire
        time.sleep(9)
        v, _ = etcd.get(lock.key)
        assert v is None

    def test_lock_refresh(self, etcd):
        lock = etcd.lock('lock-4', ttl=3)
        assert lock.acquire() is True
        assert etcd.get(lock.key)[0] is not None
        # sleep for the same total time as test_lock_expire, but refresh each
        # second
        for _ in range(9):
            time.sleep(1)
            lock.refresh()

        assert etcd.get(lock.key)[0] is not None

    def test_lock_is_acquired(self, etcd):
        lock1 = etcd.lock('lock-5', ttl=2)
        assert lock1.is_acquired() is False

        lock2 = etcd.lock('lock-5', ttl=2)
        lock2.acquire()
        assert lock2.is_acquired() is True
        lock2.release()

        lock3 = etcd.lock('lock-5', ttl=2)
        lock3.acquire()
        assert lock3.is_acquired() is True
        assert lock2.is_acquired() is False

    def test_lock_context_manager(self, etcd):
        with etcd.lock('lock-6', ttl=2) as lock:
            assert lock.is_acquired() is True
        assert lock.is_acquired() is False

    def test_lock_contended(self, etcd):
        lock1 = etcd.lock('lock-7', ttl=2)
        lock1.acquire()
        lock2 = etcd.lock('lock-7', ttl=2)
        lock2.acquire()
        assert lock1.is_acquired() is False
        assert lock2.is_acquired() is True

    def test_lock_double_acquire_release(self, etcd):
        lock = etcd.lock('lock-8', ttl=10)
        assert lock.acquire(0) is True
        assert lock.acquire(0) is False
        assert lock.release() is True

    def test_lock_acquire_none(self, etcd):
        lock = etcd.lock('lock-9', ttl=10)
        assert lock.acquire(None) is True
        # This will succeed after 10 seconds since the TTL will expire and the
        # lock is not refreshed
        assert lock.acquire(None) is True

    def test_internal_exception_on_internal_error(self, etcd):
        exception = self.MockedException(grpc.StatusCode.INTERNAL)
        kv_mock = mock.MagicMock()
        kv_mock.Range.side_effect = exception
        etcd.kvstub = kv_mock

        with pytest.raises(etcd3.exceptions.InternalServerError):
            etcd.get("foo")

    def test_connection_failure_exception_on_connection_failure(self, etcd):
        exception = self.MockedException(grpc.StatusCode.UNAVAILABLE)
        kv_mock = mock.MagicMock()
        kv_mock.Range.side_effect = exception
        etcd.kvstub = kv_mock

        with pytest.raises(etcd3.exceptions.ConnectionFailedError):
            etcd.get("foo")

    def test_connection_timeout_exception_on_connection_timeout(self, etcd):
        exception = self.MockedException(grpc.StatusCode.DEADLINE_EXCEEDED)
        kv_mock = mock.MagicMock()
        kv_mock.Range.side_effect = exception
        etcd.kvstub = kv_mock

        with pytest.raises(etcd3.exceptions.ConnectionTimeoutError):
            etcd.get("foo")

    def test_grpc_exception_on_unknown_code(self, etcd):
        exception = self.MockedException(grpc.StatusCode.DATA_LOSS)
        kv_mock = mock.MagicMock()
        kv_mock.Range.side_effect = exception
        etcd.kvstub = kv_mock

        with pytest.raises(grpc.RpcError):
            etcd.get("foo")

    def test_status_member(self, etcd):
        status = etcd.status()

        assert isinstance(status.leader, etcd3.members.Member) is True
        assert status.leader.id in [m.id for m in etcd.members]

    def test_hash(self, etcd):
        assert isinstance(etcd.hash(), int)

    def test_snapshot(self, etcd):
        with tempfile.NamedTemporaryFile() as f:
            etcd.snapshot(f)
            f.flush()

            etcdctl('snapshot', 'status', f.name)


class TestAlarms(object):
    @pytest.fixture
    def etcd(self):
        etcd = etcd3.client()
        yield etcd
        etcd.disarm_alarm()
        for m in etcd.members:
            if m.active_alarms:
                etcd.disarm_alarm(m.id)

    def test_create_alarm_all_members(self, etcd):
        alarms = etcd.create_alarm()

        assert len(alarms) == 1
        assert alarms[0].member_id == 0
        assert alarms[0].alarm_type == etcdrpc.NOSPACE

    def test_create_alarm_specific_member(self, etcd):
        a_member = next(etcd.members)

        alarms = etcd.create_alarm(member_id=a_member.id)

        assert len(alarms) == 1
        assert alarms[0].member_id == a_member.id
        assert alarms[0].alarm_type == etcdrpc.NOSPACE

    def test_list_alarms(self, etcd):
        a_member = next(etcd.members)
        etcd.create_alarm()
        etcd.create_alarm(member_id=a_member.id)
        possible_member_ids = [0, a_member.id]

        alarms = list(etcd.list_alarms())

        assert len(alarms) == 2
        for alarm in alarms:
            possible_member_ids.remove(alarm.member_id)
            assert alarm.alarm_type == etcdrpc.NOSPACE

        assert possible_member_ids == []

    def test_disarm_alarm(self, etcd):
        etcd.create_alarm()
        assert len(list(etcd.list_alarms())) == 1

        etcd.disarm_alarm()
        assert len(list(etcd.list_alarms())) == 0


class TestUtils(object):
    def test_increment_last_byte(self):
        assert etcd3.utils.increment_last_byte(b'foo') == b'fop'

    def test_to_bytes(self):
        assert isinstance(etcd3.utils.to_bytes(b'doot'), bytes) is True
        assert isinstance(etcd3.utils.to_bytes('doot'), bytes) is True
        assert etcd3.utils.to_bytes(b'doot') == b'doot'
        assert etcd3.utils.to_bytes('doot') == b'doot'


class TestEtcdTokenCallCredentials(object):

    def test_token_callback(self):
        e = EtcdTokenCallCredentials('foo')
        callback = mock.MagicMock()
        e(None, callback)
        metadata = (('token', 'foo'),)
        callback.assert_called_once_with(metadata, None)


class TestClient(object):
    @pytest.fixture
    def etcd(self):
        yield etcd3.client()

    def test_sort_target(self, etcd):
        key = 'key'.encode('utf-8')
        sort_target = {
            None: etcdrpc.RangeRequest.KEY,
            'key': etcdrpc.RangeRequest.KEY,
            'version': etcdrpc.RangeRequest.VERSION,
            'create': etcdrpc.RangeRequest.CREATE,
            'mod': etcdrpc.RangeRequest.MOD,
            'value': etcdrpc.RangeRequest.VALUE,
        }

        for input, expected in sort_target.items():
            range_request = etcd._build_get_range_request(key,
                                                          sort_target=input)
            assert range_request.sort_target == expected
        with pytest.raises(ValueError):
            etcd._build_get_range_request(key, sort_target='feelsbadman')

    def test_sort_order(self, etcd):
        key = 'key'.encode('utf-8')
        sort_target = {
            None: etcdrpc.RangeRequest.NONE,
            'ascend': etcdrpc.RangeRequest.ASCEND,
            'descend': etcdrpc.RangeRequest.DESCEND,
        }

        for input, expected in sort_target.items():
            range_request = etcd._build_get_range_request(key,
                                                          sort_order=input)
            assert range_request.sort_order == expected
        with pytest.raises(ValueError):
            etcd._build_get_range_request(key, sort_order='feelsbadman')

    def test_secure_channel(self):
        client = etcd3.client(
            ca_cert="tests/ca.crt",
            cert_key="tests/client.key",
            cert_cert="tests/client.crt"
        )
        assert client.uses_secure_channel is True

    def test_secure_channel_ca_cert_only(self):
        client = etcd3.client(
            ca_cert="tests/ca.crt",
            cert_key=None,
            cert_cert=None
        )
        assert client.uses_secure_channel is True

    def test_secure_channel_ca_cert_and_key_raise_exception(self):
        with pytest.raises(ValueError):
            etcd3.client(
                ca_cert='tests/ca.crt',
                cert_key='tests/client.crt',
                cert_cert=None)

        with pytest.raises(ValueError):
            etcd3.client(
                ca_cert='tests/ca.crt',
                cert_key=None,
                cert_cert='tests/client.crt')

    def test_compact(self, etcd):
        etcd.compact(3)
        with pytest.raises(grpc.RpcError):
            etcd.compact(3)

    def test_channel_with_no_cert(self):
        client = etcd3.client(
            ca_cert=None,
            cert_key=None,
            cert_cert=None
        )
        assert client.uses_secure_channel is False

    @mock.patch('etcdrpc.AuthStub')
    def test_user_pwd_auth(self, auth_mock):
        auth_resp_mock = mock.MagicMock()
        auth_resp_mock.token = 'foo'
        auth_mock.Authenticate = auth_resp_mock
        self._enable_auth_in_etcd()

        # Create a client using username and password auth
        client = etcd3.client(
            user='root',
            password='pwd'
        )

        assert client.call_credentials is not None
        self._disable_auth_in_etcd()

    def test_user_or_pwd_auth_raises_exception(self):
        with pytest.raises(Exception):
            etcd3.client(user='usr')

        with pytest.raises(Exception):
            etcd3.client(password='pwd')

    def _enable_auth_in_etcd(self):
        subprocess.call(['etcdctl', '-w', 'json', 'user', 'add', 'root:pwd'])
        subprocess.call(['etcdctl', 'auth', 'enable'])

    def _disable_auth_in_etcd(self):
        subprocess.call(['etcdctl', 'user', 'remove', 'root'])
        subprocess.call(['etcdctl', '-u', 'root:pwd', 'auth', 'disable'])


class TestCompares(object):

    def test_compare_version(self):
        key = 'key'
        tx = etcd3.Transactions()

        version_compare = tx.version(key) == 1
        assert version_compare.op == etcdrpc.Compare.EQUAL

        version_compare = tx.version(key) != 2
        assert version_compare.op == etcdrpc.Compare.NOT_EQUAL

        version_compare = tx.version(key) < 91
        assert version_compare.op == etcdrpc.Compare.LESS

        version_compare = tx.version(key) > 92
        assert version_compare.op == etcdrpc.Compare.GREATER
        assert version_compare.build_message().target == \
            etcdrpc.Compare.VERSION

    def test_compare_value(self):
        key = 'key'
        tx = etcd3.Transactions()

        value_compare = tx.value(key) == 'b'
        assert value_compare.op == etcdrpc.Compare.EQUAL

        value_compare = tx.value(key) != 'b'
        assert value_compare.op == etcdrpc.Compare.NOT_EQUAL

        value_compare = tx.value(key) < 'b'
        assert value_compare.op == etcdrpc.Compare.LESS

        value_compare = tx.value(key) > 'b'
        assert value_compare.op == etcdrpc.Compare.GREATER
        assert value_compare.build_message().target == etcdrpc.Compare.VALUE

    def test_compare_mod(self):
        key = 'key'
        tx = etcd3.Transactions()

        mod_compare = tx.mod(key) == -100
        assert mod_compare.op == etcdrpc.Compare.EQUAL

        mod_compare = tx.mod(key) != -100
        assert mod_compare.op == etcdrpc.Compare.NOT_EQUAL

        mod_compare = tx.mod(key) < 19
        assert mod_compare.op == etcdrpc.Compare.LESS

        mod_compare = tx.mod(key) > 21
        assert mod_compare.op == etcdrpc.Compare.GREATER
        assert mod_compare.build_message().target == etcdrpc.Compare.MOD

    def test_compare_create(self):
        key = 'key'
        tx = etcd3.Transactions()

        create_compare = tx.create(key) == 10
        assert create_compare.op == etcdrpc.Compare.EQUAL

        create_compare = tx.create(key) != 10
        assert create_compare.op == etcdrpc.Compare.NOT_EQUAL

        create_compare = tx.create(key) < 155
        assert create_compare.op == etcdrpc.Compare.LESS

        create_compare = tx.create(key) > -12
        assert create_compare.op == etcdrpc.Compare.GREATER
        assert create_compare.build_message().target == etcdrpc.Compare.CREATE
