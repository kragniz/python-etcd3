import time

import grpc


class Endpoint(object):
    """Represents an etcd cluster endpoint."""

    def __init__(self, host="localhost", port=2379, secure=False, credentials=None, time_retry=300.0,
                 opts=None):
        self.host = host
        self.netloc = "{host}:{port}".format(host=host, port=port)
        self.secure = secure
        self.protocol = 'https' if secure else 'http'
        if self.secure and credentials is None:
            raise ValueError(
                'Please set TLS credentials for secure connections')
        self.credentials = credentials
        self.time_retry = 300.0
        self.in_use = False
        self.last_failed = 0
        self.channel = self._mkchannel(opts)

    def fail(self):
        """Transition the endpoint to a failed state."""
        self.in_use = False
        self.last_failed = time.time()

    def use(self):
        """Transition the endpoint to an active state."""
        if self.is_failed():
            raise ValueError('Trying to use a failed node')
        self.in_use = True
        self.last_failed = 0
        return self.channel

    def __str__(self):
        return "Endpoint({}://{})".format(self.protocol, self.netloc)

    def is_failed(self):
        """Check if the current endpoint is failed."""
        return ((time.time() - self.last_failed) < self.time_retry)

    def _mkchannel(self, opts):
        if self.secure:
            return grpc.secure_channel(self.netloc, self.credentials,
                                       options=opts)
        else:
            return grpc.insecure_channel(self.netloc, options=opts)
