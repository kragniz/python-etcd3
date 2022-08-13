FROM python:3.9

ARG HTTP_PROXY
ARG http_proxy
ARG HTTPS_PROXY
ARG https_proxy
ENV TEST_ETCD_VERSION v3.3.10

RUN curl -L https://github.com/etcd-io/etcd/releases/download/${TEST_ETCD_VERSION}/etcd-${TEST_ETCD_VERSION}-linux-amd64.tar.gz | tar xzvf -
ENV PATH $PATH:/etcd-${TEST_ETCD_VERSION}-linux-amd64

RUN pip install -U tox

RUN mkdir python-etcd3
WORKDIR python-etcd3
# Rebuild this layer .tox when tox.ini or requirements changes
COPY tox.ini ./
COPY requirements/base.txt requirements/test.txt ./requirements/

RUN tox -epy39 --notest

COPY . ./

ENV ETCDCTL_API 3
CMD ["tox", "-epy39"]
