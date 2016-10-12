FROM python

ARG HTTP_PROXY
ARG http_proxy
ARG HTTPS_PROXY
ARG https_proxy

RUN curl -L http://github.com/coreos/etcd/releases/download/v3.0.10/etcd-v3.0.10-linux-amd64.tar.gz | tar xzvf -
ENV PATH $PATH:/etcd-v3.0.10-linux-amd64

RUN pip install -U tox

RUN mkdir python-etcd3
WORKDIR python-etcd3
# Rebuild this layer .tox when tox.ini or dev-requirements.txt changes
COPY tox.ini dev-requirements.txt ./

RUN tox -epy35 --notest

COPY . ./

ENV ETCDCTL_API 3
CMD ["tox", "-epy35"]
