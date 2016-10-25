lock_prefix = '/locks/'


class Lock(object):
    def __init__(self, name, ttl=60,
                 etcd_client=None):
        self.name = name
        self.ttl = ttl
        if etcd_client is not None:
            self.etcd_client = etcd_client

    def acquire(self):
        key = lock_prefix + self.name
        lease = self.etcd_client.lease(self.ttl)

        success = False
        attempts = 10

        while success is not True and attempts > 0:
            attempts -= 1
            # TODO: save the created revision so we can check it later to make
            # sure we still have the lock
            success, _ = self.etcd_client.transaction(
                compare=[
                    self.etcd_client.transactions.create(key) == 0
                ],
                success=[
                    self.etcd_client.transactions.put(key, '', lease=lease)
                ],
                failure=[
                    self.etcd_client.transactions.get(key)
                ]
            )

            if success is True:
                print('we got the lock!')
            else:
                print('we didn\'t get the lock :(')
