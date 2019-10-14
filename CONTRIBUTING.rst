.. highlight:: shell

============
Contributing
============

Contributions are welcome, and they are greatly appreciated! Every
little bit helps, and credit will always be given.

You can contribute in many ways:

Types of Contributions
----------------------

Report Bugs
~~~~~~~~~~~

Report bugs at https://github.com/kragniz/python-etcd3/issues.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

Fix Bugs
~~~~~~~~

Look through the GitHub issues for bugs. Anything tagged with "bug"
and "help wanted" is open to whoever wants to implement it.

Implement Features
~~~~~~~~~~~~~~~~~~

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

Write Documentation
~~~~~~~~~~~~~~~~~~~

python-etcd3 could always use more documentation, whether as part of the
official python-etcd3 docs, in docstrings, or even on the web in blog posts,
articles, and such.

Submit Feedback
~~~~~~~~~~~~~~~

The best way to send feedback is to file an issue at https://github.com/kragniz/python-etcd3/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

Get Started!
------------

Ready to contribute? Here's how to set up `python-etcd3` for local development.

1. Fork the `python-etcd3` repo on GitHub.
2. Clone your fork locally::

    $ git clone git@github.com:your_name_here/python-etcd3.git

3. Install your local copy into a virtualenv. Assuming you have virtualenvwrapper installed, this is how you set up your fork for local development::

    $ mkvirtualenv etcd3
    $ cd etcd3/
    $ python setup.py develop

4. Create a branch for local development::

    $ git checkout -b name-of-your-bugfix-or-feature

   Now you can make your changes locally.

5. When you're done making changes, check that your changes pass flake8 and the tests, including testing other Python versions with tox::

    $ flake8 etcd3 tests
    $ python setup.py test or py.test
    $ tox

   To get flake8 and tox, just pip install them into your virtualenv.

6. Commit your changes and push your branch to GitHub::

    $ git add .
    $ git commit -m "Your detailed description of your changes."
    $ git push origin name-of-your-bugfix-or-feature

7. Submit a pull request through the GitHub website.

Pull Request Guidelines
-----------------------

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.rst.
3. The pull request should work for Python 2.6, 2.7, 3.3, 3.4 and 3.5, and for PyPy. Check
   https://travis-ci.org/kragniz/python-etcd3/pull_requests
   and make sure that the tests pass for all supported Python versions.

Generating protobuf stubs
-------------------------

If the upstream protobuf files changes, copy the stubs::

    $ cp etcd/etcdserver/etcdserverpb/*.proto python-etcd3/etcd3/proto/

Then::

    $ cd python-etcd3
    $ tox -e genproto


Cutting new releases
--------------------

The release process to PyPi is automated using travis deploys and bumpversion.

1. Check changes since the last release:

   .. code-block:: bash

       $ git log $(git describe --tags --abbrev=0)..HEAD --oneline

2. Bump the version (respecting semver, one of ``major``, ``minor`` or
   ``patch``):

   .. code-block:: bash

       $ bumpversion patch

3. Push to github:

   .. code-block:: bash

       $ git push
       $ git push --tags

4. Wait for travis tests to run and deploy to PyPI


Dependency management
---------------------

This project uses ``pip-compile-multi`` (https://pypi.org/project/pip-compile-multi/) for hard-pinning dependencies versions.
Please see its documentation for usage instructions.
In short, ``requirements/base.in`` contains the list of direct requirements with occasional version constraints (like ``Django<2``)
and `requirements/base.txt` is automatically generated from it by adding recursive tree of dependencies with fixed versions.
The same goes for ``test`` and ``dev``.

To upgrade dependency versions, run ``pip-compile-multi``.

To add a new dependency without upgrade, add it to `requirements/base.in` and run `pip-compile-multi --no-upgrade`.

For installation always use ``.txt`` files. For example, command ``pip install -Ue . -r requirements/dev.txt`` will install
this project in development mode, testing requirements and development tools.
Another useful command is ``pip-sync requirements/dev.txt``, it uninstalls packages from your virtualenv that aren't listed in the file.
