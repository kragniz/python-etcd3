import functools

import grpc
import grpc._channel

import etcd3.etcdrpc as etcdrpc
import etcd3.utils as utils

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
