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
        self.uuid = None

    def acquire(self):
        lease = self.etcd_client.lease(self.ttl)

        success = False
        attempts = 10

        self.uuid = str(uuid.uuid1())

        while success is not True and attempts > 0:
            attempts -= 1
            # TODO: save the created revision so we can check it later to make
            # sure we still have the lock
            success, _ = self.etcd_client.transaction(
                compare=[
                    self.etcd_client.transactions.create(self.key) == 0
                ],
                success=[
                    self.etcd_client.transactions.put(self.key, self.uuid, lease=lease)
                ],
                failure=[
                    self.etcd_client.transactions.get(self.key)
                ]
            )
        return success

    def release(self):
        #print(self.etcd_client.get(self.key))
        success, _ = self.etcd_client.transaction(
            compare=[
                self.etcd_client.transactions.value(self.key) == \
                    self.uuid.decode('utf-8')],
            success=[self.etcd_client.transactions.delete(self.key)],
            failure=[]
        )
        if success is False:
            raise exceptions.LockAlreadyReleasedError(
                'lock "{}" does not exist'.format(self.name)
            )
