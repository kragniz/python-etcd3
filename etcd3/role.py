# Copyright 2020 Hoplite Industries, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Module containing the Role class.

The Role class is used to describe and manipulate roles within etcd.

"""


from . import client


class Perm(object):  # pylint: disable=R0205
    """Class describing an etcd permission.

    :param mode: (int) Mode of access being granted.  One of
                 :obj:`etcd3.Perms.r`, :obj:`etcd3.Perms.w`, or
                 :obj:`etcd3.Perms.rw`
    :param key: (str or bytes) Specific key or start of a range for keys in
                etcd.
    :param end: (str or bytes) Range end (can be ``None`` for a key only)

    :ivar mode: (int) Access mode being granted to the key or range.  This
                     will be one of  :obj:`etcd3.Perms.r`,
                     :obj:`etcd3.Perms.w`, or :obj:`etcd3.Perms.rw`.
    :ivar key: (bytes) Key or start of range that the permission is valid
                    for.
    :ivar end: (bytes or None) End of range that permission is valid for.

    """

    def __init__(self, mode, key, end):
        if mode not in client.Perms:
            raise ValueError("Invalid permission: %s" % repr(mode))

        self._mode = mode
        self._key = key
        self._end = end

    @property
    def mode(self):
        return self._mode

    @property
    def key(self):
        return self._key

    @property
    def end(self):
        return self._end


class Role(object):  # pylint: disable=R0205
    """Named role (permission set) for an etcd cluster.

    :param role: (str) Role name.
    :param perms: (list of object) Permission set from etcd

        The named tuple below approximates what is needed from this object

        .. code-block:: python

            Permission = namedtuple("Permission",
                                    ["permType",
                                    "key",
                                    "range_end"
                                    ]
            )
    :param etcd_client: Instance of :class:`etcd3.Etcd3Client`

    :ivar role: (str) Role name.
    :ivar perms: Dictionary of etcd key and :class:`Perm` object.

    """

    def __init__(self, name, perms, etcd_client):
        self.role = name
        self.perms = {x.key: Perm(x.permType, x.key, x.range_end)
                      for x in perms}
        self._etcd_client = etcd_client

    def __str__(self):
        return "Role {role}: perms: {perms}".format(
            role=self.role, perms=",".join(self.perms.values())
        )

    def delete(self):
        """Delete a role from the cluster."""
        self._etcd_client.delete_role(self.role)

    def grant(self, perm, key, end=None, prefix=False):
        """Grant a permission to a role.

        :param perm: (int)  One of :obj:`etcd3.Perms.r`, :obj:`etcd3.Perms.w`,
                     :obj:`etcd3.Perms.rw`.
        :param key: (str or bytes) Key or range start within etcd that the role
                    is being given access to.
        :param end: (str or bytes) If not ``None`` then this is the key range
                    end that the permission will be valid for.
        :param prefix: (bool) If True and "end" is not set add one to the
                       last byte and use it for end

        """
        self._etcd_client.grant_permission_role(
            self.role, perm, key, end=end, prefix=prefix
        )
