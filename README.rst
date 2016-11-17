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


Python client for the etcd API v3.

**Warning: this is a work in progress, many basic features do not have a stable
API.**

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
    for (event, cancel) in etcd.watch("/doot/watch"):
        print event
        watch_count += 1
        if watch_count > 10:
            cancel()

    # watch prefix
    watch_count = 0
    for (event, cancel) in etcd.watch_prefix("/doot/watch/prefix/"):
        print event
        watch_count += 1
        if watch_count > 10:
            cancel()