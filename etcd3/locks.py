import threading
import time
import uuid

from etcd3 import events, exceptions


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
    """

    lock_prefix = '/locks/'

    def __init__(self, name, ttl=60,
                 etcd_client=None):
        self.name = name
        self.ttl = ttl
        if etcd_client is not None:
            self.etcd_client = etcd_client

        self.key = self.lock_prefix + self.name
        self.lease = None
        # store uuid as bytes, since it avoids having to decode each time we
        # need to compare
        self.uuid = uuid.uuid1().bytes

    def acquire(self, timeout=10):
        """Acquire the lock.

        :params timeout: Maximum time to wait before returning. `None` means
                         forever, any other value equal or greater than 0 is
                         the number of seconds.
        :returns: True if the lock has been acquired, False otherwise.

        """
        if timeout is not None:
            deadline = time.time() + timeout

        while True:
            if self._try_acquire():
                return True

            if timeout is not None:
                remaining_timeout = max(deadline - time.time(), 0)
                if remaining_timeout == 0:
                    return False
            else:
                remaining_timeout = None

            self._wait_delete_event(remaining_timeout)

    def _try_acquire(self):
        self.lease = self.etcd_client.lease(self.ttl)

        success, metadata = self.etcd_client.transaction(
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
            self.revision = metadata[0].response_put.header.revision
            return True
        self.revision = metadata[0][0][1].mod_revision
        self.lease = None
        return False

    def _wait_delete_event(self, timeout):
        try:
            event_iter, cancel = self.etcd_client.watch(
                self.key, start_revision=self.revision + 1)
        except exceptions.WatchTimedOut:
            return

        if timeout is not None:
            timer = threading.Timer(timeout, cancel)
            timer.start()
        else:
            timer = None

        for event in event_iter:
            if isinstance(event, events.DeleteEvent):
                if timer is not None:
                    timer.cancel()

                cancel()
                break

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
        self.acquire()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.release()
