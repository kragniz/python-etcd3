class Lease(object):
    def __init__(self, id, ttl, etcd_client=None):
        self.id = id
        self.ttl = ttl

        self.etcd_client = etcd_client
