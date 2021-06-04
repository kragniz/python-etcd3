============
python-etcd3
============


.. image:: https://img.shields.io/pypi/v/etcd3.svg
        :target: https://pypi.python.org/pypi/etcd3

.. image:: https://img.shields.io/travis/kragniz/python-etcd3.svg
        :target: https://travis-ci.org/kragniz/python-etcd3

.. image:: https://readthedocs.org/projects/python-etcd3/badge/?version=latest
        :target: https://python-etcd3.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://pyup.io/repos/github/kragniz/python-etcd3/shield.svg
     :target: https://pyup.io/repos/github/kragniz/python-etcd3/
     :alt: Updates

.. image:: https://codecov.io/github/kragniz/python-etcd3/coverage.svg?branch=master
        :target: https://codecov.io/github/kragniz/python-etcd3?branch=master


Python client for the etcd API v3, supported under python 2.7, 3.4 and 3.5.

**Warning: the API is mostly stable, but may change in the future**

If you're interested in using this library, please get involved.

* Free software: Apache Software License 2.0
* Documentation: https://python-etcd3.readthedocs.io.

Basic usage:

.. code-block:: python

    import etcd3

    etcd = etcd3.client()

    etcd.get('foo')
    etcd.put('bar', 'doot')
    etcd.delete('bar')

    # locks
    lock = etcd.lock('thing')
    lock.acquire()
    # do something
    lock.release()

    with etcd.lock('doot-machine') as lock:
        # do something

    # transactions
    etcd.transaction(
        compare=[
            etcd.transactions.value('/doot/testing') == 'doot',
            etcd.transactions.version('/doot/testing') > 0,
        ],
        success=[
            etcd.transactions.put('/doot/testing', 'success'),
        ],
        failure=[
            etcd.transactions.put('/doot/testing', 'failure'),
        ]
    )

    # watch key
    watch_count = 0
    events_iterator, cancel = etcd.watch("/doot/watch")
    for event in events_iterator:
        print(event)
        watch_count += 1
        if watch_count > 10:
            cancel()

    # watch prefix
    watch_count = 0
    events_iterator, cancel = etcd.watch_prefix("/doot/watch/prefix/")
    for event in events_iterator:
        print(event)
        watch_count += 1
        if watch_count > 10:
            cancel()

    # recieve watch events via callback function
    def watch_callback(event):
        print(event)

    watch_id = etcd.add_watch_callback("/anotherkey", watch_callback)

    # cancel watch
    etcd.cancel_watch(watch_id)

    # recieve watch events for a prefix via callback function
    def watch_callback(event):
        print(event)

    watch_id = etcd.add_watch_prefix_callback("/doot/watch/prefix/", watch_callback)

    # cancel watch
    etcd.cancel_watch(watch_id)

    #
    # Enabling authentication.. it's a whole process
    #

    # Set up the base credentials for the system
    # Add root user -- don't lock your self out
    etcd.add_user("root", "SuperSecretPassword8")
    # Add root role
    etcd.add_role("root")

    # Add the root role to the root user
    user = etcd.get_user("root")
    user.grant("root")
    # Add the root role to the root user, alternate method
    etcd.grant_role("root", "root")

    # Wonder Powers Activate Authentication.
    etcd.enable_auth()

    #
    # End enabling authentication
    #

    # Now authenticate the connection as it's required now
    etcd = etcd3.client(user="root", password="SuperSecretPassword8")

    # Add normal user
    etcd.add_user("doot", "doot_is_a_password_too")

    # Add a normal role (int this case specific to the doot user by naming
    # convention.  This only has meaning if you make it have meaning...
    etcd.add_role("role.doot")

    # Start granting permissions
    role = etcd.get_role("role.doot")
    # Grant permssion to a single key
    role.grant(etc3d.Perms.rw, "/locations/doot")
    # Grant permssion to a "tree"
    role.grant(etc3d.Perms.rw, "/clients/doot/", prefix=True)
    # This creates the exact same grant.
    role.grant(etc3d.Perms.rw, "/clients/doot/", end="/clients/doot0")
    # Alternate method:
    etcd.grant_permission_role("role.doot", etc3d.Perms.rw,
        "/clients/doot/", end="/clients/doot0")

    # Assign the role to the doot user
    etcd.grant_role("doot", "role.doot")

    # Revoke a role from a user
    user = etcd.get_user("doot")
    user.revoke("role.doot")
    # Equivelent
    etcd.revoke_role("doot", "role.doot")

    # Revoke a permission from a grant
    role = etcd.get_role("doot.role")
    role.revoke("/clients/doot/", prefix=True)
    # Or equivelent 1
    role = etcd.get_role("doot.role")
    role.revoke("/clients/doot/", end="/clients/doot0")
    # Or equivelent 2
    etcd.revoke_permission_role("doot.role", "/clients/doot/",
        end="/clients/doot0")
    # Or equivelent 3
    etcd.revoke_permission_role("doot.role", "/clients/doot/", prefix=True)

    # Disable authentication
    etcd.disable_auth()
