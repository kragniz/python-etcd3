class Lease(object):
    """
    A lease.

    :ivar id: ID of the lease
    :ivar ttl: time to live for this lease
    """

    def __init__(self, id, ttl, etcd_client=None):
        self.id = id
        self.ttl = ttl

        self.etcd_client = etcd_client
