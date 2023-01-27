import asyncio
import functools

import grpc
import grpc._channel

import etcd3.etcdrpc as etcdrpc
import etcd3.exceptions as exceptions
import etcd3.utils as utils
import etcd3.watch as watch

from etcd3.client import (
    Endpoint,
    MultiEndpointEtcd3Client,
    EtcdTokenCallCredentials,
    KVMetadata,
)


class AioEndpoint(Endpoint):
    """Represents an etcd cluster endpoint for asyncio."""

    def _mkchannel(self, opts):
        if self.secure:
            return grpc.aio.secure_channel(self.netloc, self.credentials,
                                           options=opts)
        else:
            return grpc.aio.insecure_channel(self.netloc, options=opts)


class MultiEndpointEtcd3AioClient(MultiEndpointEtcd3Client):
    """etcd v3 API asyncio client with multiple endpoints."""

    def _handle_errors(payload):
        @functools.wraps(payload)
        async def handler(self, *args, **kwargs):
            try:
                return await payload(self, *args, **kwargs)
            except grpc.aio.AioRpcError as exc:
                self._manage_grpc_errors(exc)
        return handler

    def _handle_generator_errors(payload):
        @functools.wraps(payload)
        async def handler(self, *args, **kwargs):
            try:
                async for item in payload(self, *args, **kwargs):
                    yield item
            except grpc.aio.AioRpcError as exc:
                self._manage_grpc_errors(exc)
        return handler

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    def get_watcher(self):
        watchstub = etcdrpc.WatchStub(self.channel)
        return watch.AioWatcher(
            watchstub,
            timeout=self.timeout,
            call_credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    async def authenticate(self, user, password):
        """Authenticate on the server."""
        auth_request = etcdrpc.AuthenticateRequest(
            name=user,
            password=password
        )

        resp = await self.authstub.Authenticate(auth_request, self.timeout)
        self.metadata = (('token', resp.token),)
        self.call_credentials = grpc.metadata_call_credentials(
            EtcdTokenCallCredentials(resp.token))

    @_handle_errors
    async def get_response(self, key, **kwargs):
        """Get the value of a key from etcd."""
        range_request = self._build_get_range_request(
            key,
            **kwargs
        )

        return await self.kvstub.Range(
            range_request,
            timeout=self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    async def get(self, key, **kwargs):
        """
        Get the value of a key from etcd.

        example usage:

        .. code-block:: python

            >>> import etcd3
            >>> etcd = await etcd3.aioclient()
            >>> await etcd.get('/thing/key')
            'hello world'

        :param key: key in etcd to get
        :returns: value of key and metadata
        :rtype: bytes, ``KVMetadata``
        """
        range_response = await self.get_response(key, **kwargs)
        if range_response.count < 1:
            return None, None
        else:
            kv = range_response.kvs.pop()
            return kv.value, KVMetadata(kv, range_response.header)

    @_handle_errors
    async def get_prefix_response(self, key_prefix, **kwargs):
        """Get a range of keys with a prefix."""
        if any(kwarg in kwargs for kwarg in ("key", "range_end")):
            raise TypeError("Don't use key or range_end with prefix")

        range_request = self._build_get_range_request(
            key=key_prefix,
            range_end=utils.prefix_range_end(utils.to_bytes(key_prefix)),
            **kwargs
        )

        return await self.kvstub.Range(
            range_request,
            timeout=self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    async def get_prefix(self, key_prefix, **kwargs):
        """
        Get a range of keys with a prefix.

        :param key_prefix: first key in range
        :param keys_only: if True, retrieve only the keys, not the values

        :returns: sequence of (value, metadata) tuples
        """
        range_response = await self.get_prefix_response(key_prefix, **kwargs)
        return (
            (kv.value, KVMetadata(kv, range_response.header))
            for kv in range_response.kvs
        )

    @_handle_errors
    async def get_range_response(self, range_start, range_end, sort_order=None,
                                 sort_target='key', **kwargs):
        """Get a range of keys."""
        range_request = self._build_get_range_request(
            key=range_start,
            range_end=range_end,
            sort_order=sort_order,
            sort_target=sort_target,
            **kwargs
        )

        return await self.kvstub.Range(
            range_request,
            timeout=self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    async def get_range(self, range_start, range_end, **kwargs):
        """
        Get a range of keys.

        :param range_start: first key in range
        :param range_end: last key in range
        :returns: sequence of (value, metadata) tuples
        """
        range_response = await self.get_range_response(range_start, range_end,
                                                       **kwargs)
        return (
            (kv.value, KVMetadata(kv, range_response.header))
            for kv in range_response.kvs
        )

    @_handle_errors
    async def get_all_response(self, sort_order=None, sort_target='key',
                               keys_only=False):
        """Get all keys currently stored in etcd."""
        range_request = self._build_get_range_request(
            key=b'\0',
            range_end=b'\0',
            sort_order=sort_order,
            sort_target=sort_target,
            keys_only=keys_only,
        )

        return await self.kvstub.Range(
            range_request,
            timeout=self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    async def get_all(self, **kwargs):
        """
        Get all keys currently stored in etcd.

        :param keys_only: if True, retrieve only the keys, not the values
        :returns: sequence of (value, metadata) tuples
        """
        range_response = await self.get_all_response(**kwargs)
        return (
            (kv.value, KVMetadata(kv, range_response.header))
            for kv in range_response.kvs
        )

    @_handle_errors
    async def put(self, key, value, lease=None, prev_kv=False):
        """
        Save a value to etcd.

        Example usage:

        .. code-block:: python

            >>> import etcd3
            >>> etcd = etcd3.aioclient()
            >>> await etcd.put('/thing/key', 'hello world')

        :param key: key in etcd to set
        :param value: value to set key to
        :type value: bytes
        :param lease: Lease to associate with this key.
        :type lease: either :class:`.Lease`, or int (ID of lease)
        :param prev_kv: return the previous key-value pair
        :type prev_kv: bool
        :returns: a response containing a header and the prev_kv
        :rtype: :class:`.rpc_pb2.PutResponse`
        """
        put_request = self._build_put_request(key, value, lease=lease,
                                              prev_kv=prev_kv)
        return await self.kvstub.Put(
            put_request,
            timeout=self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    async def delete(self, key, prev_kv=False, return_response=False):
        """
        Delete a single key in etcd.

        :param key: key in etcd to delete
        :param prev_kv: return the deleted key-value pair
        :type prev_kv: bool
        :param return_response: return the full response
        :type return_response: bool
        :returns: True if the key has been deleted when
                  ``return_response`` is False and a response containing
                  a header, the number of deleted keys and prev_kvs when
                  ``return_response`` is True
        """
        delete_request = self._build_delete_request(key, prev_kv=prev_kv)
        delete_response = await self.kvstub.DeleteRange(
            delete_request,
            timeout=self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )
        if return_response:
            return delete_response
        return delete_response.deleted >= 1

    @_handle_errors
    async def delete_prefix(self, prefix):
        """Delete a range of keys with a prefix in etcd."""
        delete_request = self._build_delete_request(
            prefix,
            range_end=utils.prefix_range_end(utils.to_bytes(prefix))
        )
        return await self.kvstub.DeleteRange(
            delete_request,
            timeout=self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )

    @_handle_errors
    async def add_watch_callback(self, *args, **kwargs):
        """
        Watch a key or range of keys and call a callback on every response.

        If timeout was declared during the client initialization and
        the watch cannot be created during that time the method raises
        a ``WatchTimedOut`` exception.

        :param key: key to watch
        :param callback: callback function

        :returns: watch_id. Later it could be used for cancelling watch.
        """
        return await self.watcher.add_callback(*args, **kwargs)

    @_handle_errors
    async def watch_response(self, key, **kwargs):
        """
        Watch a key.

        Example usage:

        .. code-block:: python
            responses_iterator, cancel = await etcd.watch_response('/doot/key')
            async for response in responses_iterator:
                print(response)

        :param key: key to watch

        :returns: tuple of ``responses_iterator`` and ``cancel``.
                  Use ``responses_iterator`` to get the watch responses,
                  each of which contains a header and a list of events.
                  Use ``cancel`` to cancel the watch request.
        """
        response_queue = asyncio.Queue()
        canceled = asyncio.Event()

        async def callback(response):
            await response_queue.put(response)

        watch_id = await self.add_watch_callback(key, callback, **kwargs)

        async def cancel():
            canceled.set()
            await response_queue.put(None)
            await self.cancel_watch(watch_id)

        async def iterator():
            try:
                while not canceled.is_set():
                    response = await response_queue.get()
                    if response is None:
                        canceled.set()
                    if isinstance(response, Exception):
                        canceled.set()
                        raise response
                    if not canceled.is_set():
                        yield response
            except grpc.aio.AioRpcError as exc:
                self._manage_grpc_errors(exc)

        return iterator(), cancel

    async def watch(self, key, **kwargs):
        """
        Watch a key.

        Example usage:

        .. code-block:: python
            events_iterator, cancel = await etcd.watch('/doot/key')
            async for event in events_iterator:
                print(event)

        :param key: key to watch

        :returns: tuple of ``events_iterator`` and ``cancel``.
                  Use ``events_iterator`` to get the events of key changes
                  and ``cancel`` to cancel the watch request.
        """
        response_iter, cancel = await self.watch_response(key, **kwargs)
        return utils.response_to_async_event_iterator(response_iter), cancel

    async def watch_prefix_response(self, key_prefix, **kwargs):
        """
        Watch a range of keys with a prefix.

        :param key_prefix: prefix to watch

        :returns: tuple of ``responses_iterator`` and ``cancel``.
        """
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))
        return await self.watch_response(key_prefix, **kwargs)

    async def watch_prefix(self, key_prefix, **kwargs):
        """
        Watch a range of keys with a prefix.

        :param key_prefix: prefix to watch

        :returns: tuple of ``events_iterator`` and ``cancel``.
        """
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))
        return await self.watch(key_prefix, **kwargs)

    @_handle_errors
    async def watch_once_response(self, key, timeout=None, **kwargs):
        """
        Watch a key and stop after the first response.
        If the timeout was specified and response didn't arrive method
        will raise ``WatchTimedOut`` exception.
        :param key: key to watch
        :param timeout: (optional) timeout in seconds.
        :returns: ``WatchResponse``
        """
        response_queue = asyncio.Queue()

        async def callback(response):
            await response_queue.put(response)

        watch_id = await self.add_watch_callback(key, callback, **kwargs)

        try:
            return await asyncio.wait_for(response_queue.get(),
                                          timeout=timeout)
        except asyncio.TimeoutError:
            raise exceptions.WatchTimedOut()
        finally:
            await self.cancel_watch(watch_id)

    async def watch_once(self, key, timeout=None, **kwargs):
        """
        Watch a key and stop after the first event.

        If the timeout was specified and event didn't arrive method
        will raise ``WatchTimedOut`` exception.

        :param key: key to watch
        :param timeout: (optional) timeout in seconds.

        :returns: ``Event``
        """
        response = await self.watch_once_response(key, timeout=timeout, **kwargs)
        return response.events[0]

    async def watch_prefix_once_response(self, key_prefix, timeout=None, **kwargs):
        """
        Watch a range of keys with a prefix and stop after the first response.

        If the timeout was specified and response didn't arrive method
        will raise ``WatchTimedOut`` exception.
        """
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))
        return await self.watch_once_response(key_prefix, timeout=timeout, **kwargs)

    async def watch_prefix_once(self, key_prefix, timeout=None, **kwargs):
        """
        Watch a range of keys with a prefix and stop after the first event.

        If the timeout was specified and event didn't arrive method
        will raise ``WatchTimedOut`` exception.
        """
        kwargs['range_end'] = \
            utils.prefix_range_end(utils.to_bytes(key_prefix))
        return await self.watch_once(key_prefix, timeout=timeout, **kwargs)

    @_handle_errors
    async def cancel_watch(self, watch_id):
        """
        Stop watching a key or range of keys.

        :param watch_id: watch_id returned by ``add_watch_callback`` method
        """
        await self.watcher.cancel(watch_id)

    @_handle_errors
    async def compact(self, revision, physical=False):
        """
        Compact the event history in etcd up to a given revision.

        All superseded keys with a revision less than the compaction revision
        will be removed.

        :param revision: revision for the compaction operation
        :param physical: if set to True, the request will wait until the
                         compaction is physically applied to the local database
                         such that compacted entries are totally removed from
                         the backend database
        """
        compact_request = etcdrpc.CompactionRequest(revision=revision,
                                                    physical=physical)
        await self.kvstub.Compact(
            compact_request,
            timeout=self.timeout,
            credentials=self.call_credentials,
            metadata=self.metadata
        )


class Etcd3AioClient(MultiEndpointEtcd3AioClient):
    """etcd v3 API asyncio client."""

    def __init__(self, host='localhost', port=2379, ca_cert=None,
                 cert_key=None, cert_cert=None, timeout=None, user=None,
                 password=None, grpc_options=None):

        # Step 1: verify credentials
        cert_params = [c is not None for c in (cert_cert, cert_key)]
        if ca_cert is not None:
            if all(cert_params):
                credentials = self.get_secure_creds(
                    ca_cert,
                    cert_key,
                    cert_cert
                )
                self.uses_secure_channel = True
            elif any(cert_params):
                # some of the cert parameters are set
                raise ValueError(
                    'to use a secure channel ca_cert is required by itself, '
                    'or cert_cert and cert_key must both be specified.')
            else:
                credentials = self.get_secure_creds(ca_cert, None, None)
                self.uses_secure_channel = True
        else:
            self.uses_secure_channel = False
            credentials = None

        # Step 2: create Endpoint
        ep = AioEndpoint(host, port, secure=self.uses_secure_channel,
                         creds=credentials, opts=grpc_options)

        super(Etcd3AioClient, self).__init__(endpoints=[ep], timeout=timeout,
                                             user=user, password=password)


async def aioclient(host='localhost', port=2379,
                    ca_cert=None, cert_key=None, cert_cert=None, timeout=None,
                    user=None, password=None, grpc_options=None):
    """Return an instance of an Etcd3AioClient."""
    client = Etcd3AioClient(host=host,
                            port=port,
                            ca_cert=ca_cert,
                            cert_key=cert_key,
                            cert_cert=cert_cert,
                            timeout=timeout,
                            user=user,
                            password=password,
                            grpc_options=grpc_options)

    cred_params = [c is not None for c in (user, password)]
    if all(cred_params):
        await client.authenticate(user, password)
    elif any(cred_params):
        raise Exception(
            'if using authentication credentials both user and password '
            'must be specified.'
        )

    return client
