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

"""Module containing the User class.

The User class is used to describe and manipulate users within etcd.
"""


class User(object):  # pylint: disable=R0205
    """An etcd user with their roles."""

    def __init__(self, user, roles, etcd_client):
        self.user = user
        self.roles = set(roles)
        self._etcd_client = etcd_client

    def __str__(self):
        return "User {user}: roles: {roles}".format(
            user=self.user, roles=",".join(self.roles)
        )

    def delete(self):
        """Delete a user from etcd."""
        self._etcd_client.delete_user(self.user)

    def grant(self, role):
        """Grant a user a given role within etcd.

        :param role: (str) Role name to be granted to the user

        """
        self._etcd_client.grant_role(self.user, role)
        self.roles.add(role)

    def revoke(self, role):
        """Revoke a given role for a user within etcd.

        :param role: (str) Role name to be revoked from the user

        """
        self._etcd_client.revoke_role(self.user, role)
        self.roles.remove(role)
