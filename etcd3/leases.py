class Lease(object):
    """
    A lease.

    :ivar id: ID of the lease
    :ivar ttl: time to live for this lease
    """

    def __init__(self, lease_id, ttl, etcd_client=None):
        self.id = lease_id
        self.ttl = ttl

        self.etcd_client = etcd_client

    def revoke(self):
        """Revoke this lease."""
        self.etcd_client.revoke_lease(self.id)
