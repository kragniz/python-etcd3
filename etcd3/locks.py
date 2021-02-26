import uuid

import tenacity

from etcd3 import exceptions

DEFAULT_TIMEOUT = object()

class LockTimeout(Exception): pass

class Lock(object):
    """
    A distributed lock.

    This can be used as a context manager, with the lock being acquired and
    released as you would expect:

    .. code-block:: python

        etcd = etcd3.client()

        # create a lock that expires after 20 seconds
        with etcd.lock('toot', ttl=20) as lock:
            # do something that requires the lock
            print(lock.is_acquired())

            # refresh the timeout on the lease
            lock.refresh()

    :param name: name of the lock
    :type name: string or bytes
    :param ttl: length of time for the lock to live for in seconds. The lock
                will be released after this time elapses, unless refreshed
    :type ttl: int
    :param timeout: length of time to wait for this lock by default
    :type timeout: int
    """

    lock_prefix = '/locks/'

    def __init__(self, name, ttl=60, timeout=10,
                 etcd_client=None):
        self.name = name
        self.ttl = ttl
        self.default_timeout = timeout
        if etcd_client is not None:
            self.etcd_client = etcd_client

        self.key = self.lock_prefix + self.name
        self.lease = None
        # store uuid as bytes, since it avoids having to decode each time we
        # need to compare
        self.uuid = uuid.uuid1().bytes

    def acquire(self, timeout=DEFAULT_TIMEOUT, raise_on_timeout=False):
        """Acquire the lock.

        :params timeout: Maximum time to wait before returning. `None` means
                         forever, any other value equal or greater than 0 is
                         the number of seconds.
        :params raise_on_timeout: If True, will raise if timeout is reached.
        :returns: True if the lock has been acquired, False otherwise.
                  Will raise LockTimeout if and only if raise_on_timeout is
                  a True value and the timeout is reached.
        """
        if timeout is DEFAULT_TIMEOUT:
            timeout = self.default_timeout

        stop = (
            tenacity.stop_never
            if timeout is None else tenacity.stop_after_delay(timeout)
        )

        def wait(previous_attempt_number, delay_since_first_attempt):
            if timeout is None:
                remaining_timeout = None
            else:
                remaining_timeout = max(timeout - delay_since_first_attempt, 0)
            # TODO(jd): Wait for a DELETE event to happen: that'd mean the lock
            # has been released, rather than retrying on PUT events too
            try:
                self.etcd_client.watch_once(self.key, remaining_timeout)
            except exceptions.WatchTimedOut:
                pass
            return 0

        @tenacity.retry(retry=tenacity.retry_never,
                        stop=stop,
                        wait=wait)
        def _acquire():
            # TODO: save the created revision so we can check it later to make
            # sure we still have the lock

            self.lease = self.etcd_client.lease(self.ttl)

            success, _ = self.etcd_client.transaction(
                compare=[
                    self.etcd_client.transactions.create(self.key) == 0
                ],
                success=[
                    self.etcd_client.transactions.put(self.key, self.uuid,
                                                      lease=self.lease)
                ],
                failure=[
                    self.etcd_client.transactions.get(self.key)
                ]
            )
            if success is True:
                return True
            self.lease = None
            raise tenacity.TryAgain

        try:
            return _acquire()
        except tenacity.RetryError as e:
            if raise_on_timeout:
                raise LockTimeout(e)
            return False

    def release(self):
        """Release the lock."""
        success, _ = self.etcd_client.transaction(
            compare=[
                self.etcd_client.transactions.value(self.key) == self.uuid
            ],
            success=[self.etcd_client.transactions.delete(self.key)],
            failure=[]
        )
        return success

    def refresh(self):
        """Refresh the time to live on this lock."""
        if self.lease is not None:
            return self.lease.refresh()
        else:
            raise ValueError('No lease associated with this lock - have you '
                             'acquired the lock yet?')

    def is_acquired(self):
        """Check if this lock is currently acquired."""
        uuid, _ = self.etcd_client.get(self.key)

        if uuid is None:
            return False

        return uuid == self.uuid

    def __enter__(self):
        self.acquire(raise_on_timeout=True)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.release()
