"""
Tests for `etcd3` module.

----------------------------------
"""

import base64
import json
import os
import subprocess
import threading
import time

import grpc

from hypothesis import given
from hypothesis.strategies import characters

import mock

import pytest

import six
from six.moves.urllib.parse import urlparse

import etcd3
import etcd3.etcdrpc as etcdrpc
import etcd3.exceptions
import etcd3.utils as utils


etcd_version = os.environ.get('ETCD_VERSION', 'v3.0.10')

os.environ['ETCDCTL_API'] = '3'

if six.PY2:
    int_types = (int, long)
else:
    int_types = (int,)


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
        timeout = 5
        if endpoint:
            url = urlparse(endpoint)
            yield etcd3.client(host=url.hostname,
                               port=url.port,
                               timeout=timeout)
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
        returned, _ = etcd.get('/doot/a_key')
        assert returned == string.encode('utf-8')

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_get_random_key(self, etcd, string):
        etcdctl('put', '/doot/' + string, 'dootdoot')
        returned, _ = etcd.get('/doot/' + string)
        assert returned == b'dootdoot'

    @given(characters(blacklist_categories=['Cs', 'Cc']))
    def test_put_key(self, etcd, string):
        etcd.put('/doot/put_1', string)
        out = etcdctl('get', '/doot/put_1')
        assert base64.b64decode(out['kvs'][0]['value']) == \
            string.encode('utf-8')

    def test_delete_key(self, etcd):
        etcdctl('put', '/doot/delete_this', 'delete pls')

        v, _ = etcd.get('/doot/delete_this')
        assert v == b'delete pls'

        etcd.delete('/doot/delete_this')

        with pytest.raises(etcd3.exceptions.KeyNotFoundError):
            etcd.get('/doot/delete_this')

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

    def test_all_not_found_error(self, etcd):
        with pytest.raises(etcd3.exceptions.KeyNotFoundError):
            list(etcd.get_all())

    def test_range_not_found_error(self, etcd):
        for i in range(5):
            etcdctl('put', '/doot/notrange{}'.format(i), 'i am a not range')

        with pytest.raises(etcd3.exceptions.KeyNotFoundError):
            list(etcd.get_prefix('/doot/range'))

    def test_get_all(self, etcd):
        for i in range(20):
            etcdctl('put', '/doot/range{}'.format(i), 'i am in all')

        for i in range(5):
            etcdctl('put', '/doot/notrange{}'.format(i), 'i am in all')
        values = list(etcd.get_all())
        assert len(values) == 25
        for value, _ in values:
            assert value == b'i am in all'

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

    def test_lease_grant(self, etcd):
        lease = etcd.lease(1)

        assert isinstance(lease.ttl, int_types)
        assert isinstance(lease.id, int_types)

    def test_lease_revoke(self, etcd):
        lease = etcd.lease(1)
        lease.revoke()

    @pytest.mark.skipif(not etcd_version.startswith('v3.1'),
                        reason="requires etcd v3.1")
    def test_lease_keys_empty(self, etcd):
        lease = etcd.lease(1)
        assert lease.keys == []

    @pytest.mark.skipif(not etcd_version.startswith('v3.1'),
                        reason="requires etcd v3.1")
    def test_lease_single_key(self, etcd):
        lease = etcd.lease(1)
        etcd.put('/doot/lease_test', 'this is a lease', lease=lease)
        assert lease.keys == [b'/doot/lease_test']

    @pytest.mark.skipif(not etcd_version.startswith('v3.1'),
                        reason="requires etcd v3.1")
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
        with pytest.raises(etcd3.exceptions.KeyNotFoundError):
            etcd.get(key)

    def test_member_list_single(self, etcd):
        # if tests are run against an etcd cluster rather than a single node,
        # this test will need to be changed
        assert len(list(etcd.members)) == 1
        for member in etcd.members:
            assert member.name == 'default'
            for peer_url in member.peer_urls:
                assert peer_url.startswith('http://')
            for client_url in member.client_urls:
                assert client_url.startswith('http://')
            assert isinstance(member.id, int_types) is True

    def test_lock_acquire(self, etcd):
        lock = etcd.lock('lock-1', ttl=10)
        assert lock.acquire() is True
        assert etcd.get(lock.key)[0] is not None

    def test_lock_release(self, etcd):
        lock = etcd.lock('lock-2', ttl=10)
        assert lock.acquire() is True
        assert etcd.get(lock.key)[0] is not None
        assert lock.release() is True
        with pytest.raises(etcd3.exceptions.KeyNotFoundError):
            etcd.get(lock.key)

    def test_lock_expire(self, etcd):
        lock = etcd.lock('lock-3', ttl=2)
        assert lock.acquire() is True
        assert etcd.get(lock.key)[0] is not None
        # wait for the lease to expire
        time.sleep(6)
        with pytest.raises(etcd3.exceptions.KeyNotFoundError):
            etcd.get(lock.key)

    def test_lock_refresh(self, etcd):
        lock = etcd.lock('lock-4', ttl=2)
        assert lock.acquire() is True
        assert etcd.get(lock.key)[0] is not None
        # sleep for the same total time as test_lock_expire, but refresh each
        # second
        for _ in range(6):
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

    def test_proper_exception_on_internal_error(self, etcd):
        class MockedException(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.INTERNAL
        kv_mock = mock.MagicMock()
        kv_mock.Range.side_effect = MockedException()
        etcd.kvstub = kv_mock

        with pytest.raises(etcd3.exceptions.InternalServerErrorException):
            etcd.get("foo")

    def test_proper_exception_on_connection_failure(self, etcd):
        class MockedException(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE
        kv_mock = mock.MagicMock()
        kv_mock.Range.side_effect = MockedException()
        etcd.kvstub = kv_mock

        with pytest.raises(etcd3.exceptions.ConnectionFailedException):
            etcd.get("foo")

    def test_proper_exception_on_connection_timeout(self, etcd):
        class MockedException(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.DEADLINE_EXCEEDED
        kv_mock = mock.MagicMock()
        kv_mock.Range.side_effect = MockedException()
        etcd.kvstub = kv_mock

        with pytest.raises(etcd3.exceptions.ConnectionTimeoutException):
            etcd.get("foo")


class TestUtils(object):
    def test_increment_last_byte(self):
        assert etcd3.utils.increment_last_byte(b'foo') == b'fop'

    def test_to_bytes(self):
        assert isinstance(etcd3.utils.to_bytes(b'doot'), bytes) is True
        assert isinstance(etcd3.utils.to_bytes('doot'), bytes) is True
        assert etcd3.utils.to_bytes(b'doot') == b'doot'
        assert etcd3.utils.to_bytes('doot') == b'doot'


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
        client = etcd3.client(ca_cert="tests/ca.crt",
                              cert_key="tests/client.key",
                              cert_cert="tests/client.crt")
        assert client.uses_secure_channel is True


class TestCompares(object):

    def test_compare_version(self):
        key = 'key'
        tx = etcd3.Transactions()

        version_compare = tx.version(key) == 1
        assert version_compare.op == etcdrpc.Compare.EQUAL

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

        create_compare = tx.create(key) < 155
        assert create_compare.op == etcdrpc.Compare.LESS

        create_compare = tx.create(key) > -12
        assert create_compare.op == etcdrpc.Compare.GREATER
        assert create_compare.build_message().target == etcdrpc.Compare.CREATE
