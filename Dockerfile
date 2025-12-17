FROM python:3.10

ARG HTTP_PROXY
ARG http_proxy
ARG HTTPS_PROXY
ARG https_proxy

RUN mkdir /python-etcd3

ENV TEST_ETCD_VERSION=v3.6.6
ENV UV_CACHE_DIR=/python-etcd3/.cache/uv

RUN curl -L https://github.com/etcd-io/etcd/releases/download/${TEST_ETCD_VERSION}/etcd-${TEST_ETCD_VERSION}-linux-amd64.tar.gz | tar xzvf -
ENV PATH=$PATH:/etcd-${TEST_ETCD_VERSION}-linux-amd64

COPY --from=ghcr.io/astral-sh/uv:0.9.16 /uv /uvx /bin/
ENV PATH=$PATH:/etcd-${TEST_ETCD_VERSION}-linux-amd64
COPY . .
RUN uv sync

WORKDIR /python-etcd3
COPY tox.ini ./
RUN uv run tox -epy310 --notest

COPY . ./

CMD ["uv", "run", "tox", "-epy310"]
