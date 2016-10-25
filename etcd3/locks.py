class Lock(object):
    def __init__(self, name, ttl=60,
                 etcd_client=None):
        self.name = name
        self.ttl = ttl
        self.etcd_client = etcd_client
