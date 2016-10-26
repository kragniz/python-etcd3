import uuid

import etcd3.exceptions as exceptions

lock_prefix = '/locks/'


class Lock(object):
    def __init__(self, name, ttl=60,
                 etcd_client=None):
        self.name = name
        self.ttl = ttl
        if etcd_client is not None:
            self.etcd_client = etcd_client

        self.key = lock_prefix + self.name
        self.lease = None
        self.uuid = None

    def acquire(self):
        print('acquiring', self.name)
        self.lease = self.etcd_client.lease(self.ttl)

        success = False
        attempts = 10

        # store uuid as bytes, since it avoids having to decode each time we
        # need to compare
        self.uuid = str(uuid.uuid1()).decode('utf-8')

        while success is not True and attempts > 0:
            attempts -= 1
            # TODO: save the created revision so we can check it later to make
            # sure we still have the lock
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
        return success

    def release(self):
        print('releasing', self.name)
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
        try:
            uuid = self.etcd_client.get(self.key)
        except exceptions.KeyNotFoundError:
            return False

        if uuid == self.uuid:
            return True
        else:
            return False

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.release()
