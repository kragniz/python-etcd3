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


Python client for the etcd3 API

* Free software: Apache Software License 2.0
* Documentation: https://python-etcd3.readthedocs.io.

This project is developed using Readme Driven Development.

Fictitious example of api usage:

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

    # watching
    for event in etcd.watch('some-key'):
        print(event)
