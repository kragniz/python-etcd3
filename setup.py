#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from setuptools import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()


def load_reqs(filename):
    with open(filename) as reqs_file:
        return [
            re.sub('==', '>=', line) for line in reqs_file.readlines()
            if not re.match('\s*#', line)
        ]

requirements = load_reqs('requirements.txt')
test_requirements = load_reqs('test-requirements.txt')

setup(
    name='etcd3',
    version='0.2.0',
    description="Python client for the etcd3 API",
    long_description=readme + '\n\n' + history,
    author="Louis Taylor",
    author_email='louis@kragniz.eu',
    url='https://github.com/kragniz/python-etcd3',
    packages=[
        'etcd3',
    ],
    package_dir={'etcd3':
                 'etcd3'},
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='etcd3',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
