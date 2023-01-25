import functools

import grpc
import grpc._channel

import etcd3.etcdrpc as etcdrpc

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
            >>> etcd = await etcd3.client()
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
