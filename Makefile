include MakefileHelp.mk

ETCD_DOWNLOAD_URL?=https://github.com/etcd-io/etcd/releases/download
ETCD_PROTO_SOURCE_URL?=https://raw.githubusercontent.com/etcd-io/jetcd/main/jetcd-grpc/src/main/proto/
TEST_ETCD_VERSION?=v3.3.10
TEST_ETCD_DOCKER_HOST?=host.docker.internal

install: ## install local development dependencies
	@poetry install
.PHONY: install

uninstall: ## uninstalls local development dependencies
	@if [ -d .venv ]; then rm -rf .venv; fi
.PHONY: uninstall

clean: ## removes intermediate files and build artifacts
	@if [ -d .etcd ]; then rm -rf .etcd; fi
	@if [ -d .pytest_cache ]; then rm -rf .pytest_cache; fi
	@if [ -d .hypothesis ]; then rm -rf .hypothesis; fi
	@if [ -f .coverage ]; then rm .coverage; fi
	@if [ -d dist ]; then rm -rf dist; fi
	@find . -name '*.pyc' -delete
	@find . -name '__pycache__' -delete
.PHONY: clean

lint:  ## lints all code
	@poetry run flake8
.PHONY: lint

test: .etcd ## runs the tests
	@PATH="$(PWD)/.etcd:$(PATH)"; \
		TEST_ETCD_VERSION="$(TEST_ETCD_VERSION)"; \
		poetry run pifpaf run etcd --cluster -- pytest -- --cov -v
.PHONY: test

build:  ## builds the project to tar & wheel formats
	@poetry build

.etcd:  ## installs etcd locally for running
	@mkdir -p .etcd
ifeq ($(shell uname -s),Linux)
	@curl -L -s \
		$(ETCD_DOWNLOAD_URL)/$(TEST_ETCD_VERSION)/etcd-$(TEST_ETCD_VERSION)-linux-amd64.tar.gz \
		| tar xz -C .etcd
	@mv .etcd/etcd-$(TEST_ETCD_VERSION)-linux-amd64/* .etcd/
	@rm -r .etcd/etcd-$(TEST_ETCD_VERSION)-linux-amd64
else ifeq ($(shell uname -s),Darwin)
	@curl -L -s \
		$(ETCD_DOWNLOAD_URL)/$(TEST_ETCD_VERSION)/etcd-$(TEST_ETCD_VERSION)-darwin-amd64.zip \
		-o .etcd/etcd-$(TEST_ETCD_VERSION)-darwin-amd64.zip
	@unzip -q -d .etcd .etcd/etcd-$(TEST_ETCD_VERSION)-darwin-amd64.zip
	@mv .etcd/etcd-$(TEST_ETCD_VERSION)-darwin-amd64/* .etcd/
	@rm -r .etcd/etcd-$(TEST_ETCD_VERSION)-darwin-amd64
	@rm .etcd/etcd-$(TEST_ETCD_VERSION)-darwin-amd64.zip
endif

etcd3/proto:  ## clones the protobuf definitions
	@mkdir -p etcd3/proto
	@curl -L -s $(ETCD_PROTO_SOURCE_URL)/auth.proto -o etcd3/proto/auth.proto
	@curl -L -s $(ETCD_PROTO_SOURCE_URL)/kv.proto -o etcd3/proto/kv.proto
	@curl -L -s $(ETCD_PROTO_SOURCE_URL)/rpc.proto -o etcd3/proto/rpc.proto

etcd3/etcdrpc: etcd3/proto  ## generates etcd protobuf definitions
	@mkdir -p etcd3/etcdrpc
	@sed -i -e '/gogoproto/d' etcd3/proto/rpc.proto
	@sed -i -e 's/etcd\/mvcc\/mvccpb\/kv.proto/kv.proto/g' etcd3/proto/rpc.proto
	@sed -i -e 's/etcd\/auth\/authpb\/auth.proto/auth.proto/g' etcd3/proto/rpc.proto
	@sed -i -e '/google\/api\/annotations.proto/d' etcd3/proto/rpc.proto
	@sed -i -e '/option (google.api.http)/,+3d' etcd3/proto/rpc.proto
	@poetry run python -m grpc.tools.protoc -Ietcd3/proto \
		--python_out=etcd3/etcdrpc/ \
		--grpc_python_out=etcd3/etcdrpc/ \
		etcd3/proto/rpc.proto etcd3/proto/auth.proto etcd3/proto/kv.proto
	@sed -i -e 's/import auth_pb2/from etcd3.etcdrpc import auth_pb2/g' etcd3/etcdrpc/rpc_pb2.py
	@sed -i -e 's/import kv_pb2/from etcd3.etcdrpc import kv_pb2/g' etcd3/etcdrpc/rpc_pb2.py
	@sed -i -e 's/import rpc_pb2/from etcd3.etcdrpc import rpc_pb2/g' etcd3/etcdrpc/rpc_pb2_grpc.py
	@rm etcd3/etcdrpc/auth_pb2_grpc.py
	@rm etcd3/etcdrpc/kv_pb2_grpc.py
.PHONY: etcd3/etcdrpc

etcd-up: ## starts etcd
	@docker run -d --rm \
		--name etcd \
		-p 2379:2379 \
		-p 2380:2380 \
		-e ETCD_NAME="s1" \
		-e ETCD_DATA_DIR="/etc-data" \
		-e ETCD_LISTEN_CLIENT_URLS="http://0.0.0.0:2379" \
		-e ETCD_ADVERTISE_CLIENT_URLS="http://0.0.0.0:2379" \
		-e ETCD_LISTEN_PEER_URLS="http://0.0.0.0:2380" \
		-e ETCD_INITIAL_ADVERTISE_PEER_URLS="http://0.0.0.0:2380" \
		-e ETCD_INITIAL_CLUSTER="s1=http://0.0.0.0:2380" \
		-e ETCD_INITIAL_CLUSTER_TOKEN="tkn" \
		-e ETCD_INITIAL_CLUSTER_STATE="new" \
		gcr.io/etcd-development/etcd:$(TEST_ETCD_VERSION)
.PHONY: etcd-up

etcd-down: ## stops etcd
	@docker stop etcd
.PHONY: etcd-down
