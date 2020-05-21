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

    async def _get_lease_info(self, *, keys=True):
        return await self.etcd_client.get_lease_info(self.id, keys=keys)

    async def revoke(self):
        """Revoke this lease."""
        await self.etcd_client.revoke_lease(self.id)

    async def refresh(self):
        """Refresh the time to live for this lease."""
        return await self.etcd_client.refresh_lease(self.id)

    # @property
    async def remaining_ttl(self):
        return (await self._get_lease_info(keys=False)).TTL

    # @property
    async def granted_ttl(self):
        return (await self._get_lease_info(keys=False)).grantedTTL

    # @property
    async def keys(self):
        return (await self._get_lease_info()).keys
