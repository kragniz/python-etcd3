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

    def _get_lease_info(self):
        return self.etcd_client.get_lease_info(self.id)

    def revoke(self):
        """Revoke this lease."""
        self.etcd_client.revoke_lease(self.id)

    def refresh(self):
        """Refresh the time to live for this lease."""
        return list(self.etcd_client.refresh_lease(self.id))

    @property
    def remaining_ttl(self):
        return self._get_lease_info().TTL

    @property
    def granted_ttl(self):
        return self._get_lease_info().grantedTTL

    @property
    def keys(self):
        return self._get_lease_info().keys


class AioLease(Lease):
    """
    An asyncio lease.

    :ivar id: ID of the lease
    :ivar ttl: time to live for this lease
    """

    async def _get_lease_info(self):
        return await self.etcd_client.get_lease_info(self.id)

    async def revoke(self):
        """Revoke this lease."""
        await self.etcd_client.revoke_lease(self.id)

    async def refresh(self):
        """Refresh the time to live for this lease."""
        return [response async for response in self.etcd_client.refresh_lease(self.id)]

    async def get_remaining_ttl(self):
        """Retrieve the remaining ttl for this lease."""
        info = await self._get_lease_info()
        return info.TTL

    async def get_granted_ttl(self):
        """Retrieve the initial granted ttl for this lease."""
        info = await self._get_lease_info()
        return info.grantedTTL

    async def get_keys(self):
        """Retrieve all keys associated with this lease."""
        info = await self._get_lease_info()
        return info.keys

    @property
    def remaining_ttl(self):
        raise NotImplementedError(
            "Use the coroutine method AioLease.get_remaining_ttl() instead.")

    @property
    def granted_ttl(self):
        raise NotImplementedError(
            "Use the coroutine method AioLease.get_granted_ttl() instead.")

    @property
    def keys(self):
        raise NotImplementedError(
            "Use the coroutine method AioLease.get_keys() instead.")
