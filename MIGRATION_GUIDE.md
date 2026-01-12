# Migration Guide: Protobuf 3.17.0 → 6.33.3

This guide helps you migrate your application to use python-etcd3 with protobuf 6.33.3.

---

## Prerequisites

Before starting the migration, ensure:

- ✅ Your application can run on Python 3.9 or higher
- ✅ You have a test environment with etcd server
- ✅ You have comprehensive test coverage for etcd operations
- ✅ You have reviewed the breaking changes in [PROTOBUF_UPGRADE_REPORT.md](PROTOBUF_UPGRADE_REPORT.md)

---

## Migration Checklist

### Phase 1: Assessment (Day 1)

- [ ] **Check Python version compatibility**
  ```bash
  python3 --version
  # Must output 3.9.0 or higher
  ```

- [ ] **Audit current Python version usage**
  ```bash
  # Check all environments
  - Development machines
  - CI/CD pipelines
  - Staging servers
  - Production servers
  - Docker images
  ```

- [ ] **Review dependencies**
  ```bash
  pip list | grep -E "(protobuf|grpc)"
  # Check for conflicts with other packages
  ```

- [ ] **Identify affected code**
  ```bash
  # Search for direct protobuf usage
  grep -r "from google.protobuf" .
  grep -r "import etcd3.etcdrpc" .
  ```

### Phase 2: Development Environment (Day 1-2)

- [ ] **Upgrade Python runtime**
  ```bash
  # Using pyenv (recommended)
  pyenv install 3.12.9
  pyenv local 3.12.9

  # Or using system package manager
  # Ubuntu/Debian:
  sudo apt-get install python3.12

  # macOS:
  brew install python@3.12
  ```

- [ ] **Create new virtual environment**
  ```bash
  python3.12 -m venv venv-py312
  source venv-py312/bin/activate
  ```

- [ ] **Install upgraded dependencies**
  ```bash
  pip install --upgrade pip
  pip install etcd3>=0.12.0
  # Or from source with updated requirements
  pip install -r requirements/base.txt
  ```

- [ ] **Verify installation**
  ```python
  import google.protobuf
  import grpc
  import etcd3

  print(f"protobuf: {google.protobuf.__version__}")  # Should be 6.33.3
  print(f"grpcio: {grpc.__version__}")                # Should be 1.76.0
  ```

### Phase 3: Code Changes (Day 2-3)

#### 3.1 Update Python Version Declarations

- [ ] **Update setup.py / pyproject.toml**
  ```python
  # setup.py
  python_requires='>=3.9',
  classifiers=[
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: 3.9',
      'Programming Language :: Python :: 3.10',
      'Programming Language :: Python :: 3.11',
      'Programming Language :: Python :: 3.12',
  ]
  ```

- [ ] **Update tox.ini**
  ```ini
  [tox]
  envlist = py39,py310,py311,py312
  ```

- [ ] **Update .python-version**
  ```
  3.12.9
  ```

#### 3.2 Remove Python 2 Compatibility Code

- [ ] **Remove six usage** (if present)
  ```python
  # Before
  import six
  if six.PY2:
      # Python 2 code

  # After
  # Remove six import and Python 2 branches
  ```

- [ ] **Update string/bytes handling**
  ```python
  # Before (Python 2 compatible)
  if isinstance(value, (str, unicode)):
      value = value.encode('utf-8')

  # After (Python 3 only)
  if isinstance(value, str):
      value = value.encode('utf-8')
  ```

#### 3.3 Update Protobuf Code (if using generated code directly)

- [ ] **Review enum handling**
  ```python
  # Protobuf 6.x enforces closed enum validation
  # Ensure all enum values are valid

  # Before - might silently accept invalid values
  request.sort_order = 999  # Invalid value

  # After - will raise error
  request.sort_order = rpc_pb2.RangeRequest.ASCEND
  ```

- [ ] **Update message creation**
  ```python
  # No changes needed - API is backward compatible
  request = rpc_pb2.RangeRequest()
  request.key = b'my_key'
  ```

### Phase 4: Testing (Day 3-5)

#### 4.1 Unit Tests

- [ ] **Run existing unit tests**
  ```bash
  pytest tests/unit/ -v
  ```

- [ ] **Add protobuf-specific tests**
  ```python
  def test_protobuf_message_creation():
      """Verify protobuf messages can be created."""
      from etcd3.etcdrpc import rpc_pb2

      request = rpc_pb2.RangeRequest()
      request.key = b'test_key'
      assert request.key == b'test_key'

  def test_protobuf_version():
      """Verify protobuf version is 6.x."""
      import google.protobuf
      major = int(google.protobuf.__version__.split('.')[0])
      assert major == 6
  ```

#### 4.2 Integration Tests

- [ ] **Test with real etcd server**
  ```bash
  # Start etcd
  docker run -d -p 2379:2379 \
    --name etcd-test \
    quay.io/coreos/etcd:v3.5 \
    /usr/local/bin/etcd \
    --advertise-client-urls http://0.0.0.0:2379 \
    --listen-client-urls http://0.0.0.0:2379

  # Run integration tests
  PYTHON_ETCD_HTTP_URL=http://localhost:2379 pytest tests/integration/
  ```

- [ ] **Test all etcd operations**
  ```python
  def test_full_workflow():
      client = etcd3.client()

      # Test put/get
      client.put('key1', 'value1')
      value, meta = client.get('key1')
      assert value == b'value1'

      # Test lease
      lease = client.lease(ttl=10)
      client.put('key2', 'value2', lease=lease)

      # Test transaction
      txn = client.transactions
      success, _ = client.transaction(
          compare=[txn.value('key1') == b'value1'],
          success=[txn.put('key1', 'updated')],
          failure=[]
      )
      assert success

      # Test watch
      watch_id, cancel = client.watch('key1')
      cancel()

      # Cleanup
      client.delete_prefix('key')
  ```

#### 4.3 Performance Tests

- [ ] **Benchmark critical operations**
  ```python
  import time

  def benchmark_put_operations(client, iterations=1000):
      start = time.time()
      for i in range(iterations):
          client.put(f'bench/key_{i}', f'value_{i}')
      elapsed = time.time() - start
      print(f"Put operations: {iterations} in {elapsed:.2f}s")
      print(f"Throughput: {iterations/elapsed:.2f} ops/s")
      return elapsed

  # Compare with baseline
  client = etcd3.client()
  elapsed = benchmark_put_operations(client)
  # Should be similar or better than protobuf 3.x
  ```

### Phase 5: CI/CD Updates (Day 4-5)

#### 5.1 Update CI Configuration

- [ ] **GitHub Actions**
  ```yaml
  # .github/workflows/test.yml
  name: Test

  on: [push, pull_request]

  jobs:
    test:
      runs-on: ubuntu-latest
      strategy:
        matrix:
          python-version: ['3.9', '3.10', '3.11', '3.12']

      steps:
        - uses: actions/checkout@v3

        - name: Set up Python ${{ matrix.python-version }}
          uses: actions/setup-python@v4
          with:
            python-version: ${{ matrix.python-version }}

        - name: Install dependencies
          run: |
            pip install -r requirements/base.txt
            pip install -r requirements/test.txt

        - name: Run tests
          run: pytest tests/
  ```

- [ ] **GitLab CI**
  ```yaml
  # .gitlab-ci.yml
  test:
    image: python:3.12
    script:
      - pip install -r requirements/base.txt
      - pip install -r requirements/test.txt
      - pytest tests/
    parallel:
      matrix:
        - PYTHON_VERSION: ['3.9', '3.10', '3.11', '3.12']
  ```

- [ ] **Jenkins**
  ```groovy
  pipeline {
      agent {
          docker {
              image 'python:3.12'
          }
      }
      stages {
          stage('Test') {
              steps {
                  sh 'pip install -r requirements/base.txt'
                  sh 'pip install -r requirements/test.txt'
                  sh 'pytest tests/'
              }
          }
      }
  }
  ```

#### 5.2 Update Docker Images

- [ ] **Update Dockerfile**
  ```dockerfile
  # Before
  FROM python:3.6-slim

  # After
  FROM python:3.12-slim

  WORKDIR /app
  COPY requirements/base.txt .
  RUN pip install --no-cache-dir -r base.txt

  COPY . .
  CMD ["python", "app.py"]
  ```

- [ ] **Update docker-compose.yml**
  ```yaml
  version: '3.8'
  services:
    app:
      build:
        context: .
        dockerfile: Dockerfile
      environment:
        - PYTHON_VERSION=3.12
      depends_on:
        - etcd

    etcd:
      image: quay.io/coreos/etcd:v3.5
      ports:
        - "2379:2379"
      command:
        - /usr/local/bin/etcd
        - --advertise-client-urls=http://0.0.0.0:2379
        - --listen-client-urls=http://0.0.0.0:2379
  ```

### Phase 6: Staging Deployment (Day 6-7)

- [ ] **Deploy to staging**
  ```bash
  # Update Python runtime
  # Deploy application with new dependencies
  # Monitor logs for errors
  ```

- [ ] **Smoke tests**
  ```bash
  # Test critical paths
  curl https://staging.example.com/health
  curl https://staging.example.com/api/test-etcd
  ```

- [ ] **Monitor metrics**
  - Response times
  - Error rates
  - Resource usage (CPU, memory)
  - etcd connection pool stats

- [ ] **Load testing**
  ```bash
  # Use load testing tool (e.g., locust, k6, ab)
  ab -n 10000 -c 100 https://staging.example.com/api/
  ```

### Phase 7: Production Deployment (Day 8+)

- [ ] **Pre-deployment checklist**
  - [ ] All tests passing
  - [ ] Staging environment stable for 24+ hours
  - [ ] Rollback plan documented
  - [ ] Team notified
  - [ ] Maintenance window scheduled (if needed)

- [ ] **Deploy to production**
  ```bash
  # Blue-green deployment recommended
  # Or canary deployment with gradual rollout
  ```

- [ ] **Post-deployment monitoring**
  - [ ] Monitor error rates for 1 hour
  - [ ] Check etcd operation latencies
  - [ ] Verify no memory leaks
  - [ ] Review logs for warnings/errors

- [ ] **Rollback if needed**
  ```bash
  # Revert to previous version if issues detected
  git revert <commit-hash>
  # Or use deployment system rollback
  kubectl rollout undo deployment/myapp
  ```

---

## Common Migration Scenarios

### Scenario 1: Simple Application (No Direct Protobuf Usage)

**Characteristics:**
- Uses etcd3 client for key-value operations only
- No direct protobuf message handling
- No custom protobuf definitions

**Migration Steps:**
1. Upgrade Python to 3.9+
2. Update dependencies: `pip install --upgrade etcd3`
3. Run tests
4. Deploy

**Estimated Time:** 1-2 days

### Scenario 2: Application with Direct Protobuf Usage

**Characteristics:**
- Creates protobuf messages directly
- Uses protobuf serialization/deserialization
- May have custom .proto files

**Migration Steps:**
1. Upgrade Python to 3.9+
2. Review enum usage and validation
3. Regenerate custom protobuf stubs
4. Update protobuf message creation code
5. Extensive testing
6. Deploy

**Estimated Time:** 3-5 days

### Scenario 3: Microservices Architecture

**Characteristics:**
- Multiple services using etcd3
- Different Python versions across services
- Complex deployment pipeline

**Migration Steps:**
1. Create migration plan for all services
2. Identify service dependencies
3. Migrate services in dependency order
4. Update shared libraries first
5. Rolling deployment per service
6. Monitor each service after deployment

**Estimated Time:** 1-2 weeks

---

## Troubleshooting

### Issue 1: Import Errors

**Symptom:**
```
ImportError: cannot import name '_message' from 'google.protobuf'
```

**Solution:**
```bash
# Reinstall protobuf
pip uninstall protobuf
pip install protobuf==6.33.3

# Clear Python cache
find . -type d -name __pycache__ -exec rm -rf {} +
find . -type f -name "*.pyc" -delete
```

### Issue 2: Version Conflicts

**Symptom:**
```
ERROR: pip's dependency resolver does not currently take into account all the packages that are installed.
```

**Solution:**
```bash
# Check conflicting packages
pip check

# Upgrade all packages
pip install --upgrade pip
pip install --upgrade -r requirements/base.txt

# Or use fresh virtualenv
python3 -m venv venv-new
source venv-new/bin/activate
pip install -r requirements/base.txt
```

### Issue 3: gRPC Connection Errors

**Symptom:**
```
grpc._channel._InactiveRpcError: StatusCode.UNAVAILABLE
```

**Solution:**
```python
# Check etcd server is running
import socket
sock = socket.socket()
try:
    sock.connect(('localhost', 2379))
    print("etcd server is reachable")
except Exception as e:
    print(f"Cannot connect to etcd: {e}")
finally:
    sock.close()

# Verify client configuration
client = etcd3.client(host='localhost', port=2379, timeout=5)
```

### Issue 4: Enum Validation Errors

**Symptom:**
```
ValueError: Unknown enum value: 999
```

**Solution:**
```python
# Use valid enum values only
from etcd3.etcdrpc import rpc_pb2

# Wrong
request.sort_order = 999

# Correct
request.sort_order = rpc_pb2.RangeRequest.ASCEND
# Or
request.sort_order = rpc_pb2.RangeRequest.SortOrder.ASCEND
```

### Issue 5: Performance Regression

**Symptom:**
Operations are slower after upgrade.

**Solution:**
```python
# 1. Check if you're using the correct protobuf implementation
import google.protobuf
print(google.protobuf.__version__)  # Should be 6.33.3

# 2. Profile your code
import cProfile
import pstats

profiler = cProfile.Profile()
profiler.enable()

# Your etcd operations here
client.put('key', 'value')

profiler.disable()
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(10)

# 3. Consider connection pooling if making many connections
# Use context manager or reuse client instance
with etcd3.client() as client:
    for i in range(1000):
        client.put(f'key_{i}', f'value_{i}')
```

---

## Rollback Procedure

If critical issues arise, follow this rollback procedure:

### Step 1: Identify the Issue

```bash
# Check logs
tail -f /var/log/application.log | grep ERROR

# Check metrics
# - Error rate spike?
# - Latency increase?
# - Memory leak?
```

### Step 2: Decide on Rollback

Rollback if:
- ✅ Error rate >5%
- ✅ Critical functionality broken
- ✅ Performance degradation >30%
- ✅ Memory leak detected

### Step 3: Execute Rollback

```bash
# Option A: Git revert
git revert <upgrade-commit-hash>
git push origin main

# Option B: Deployment system
kubectl rollout undo deployment/myapp
# or
docker service update --rollback myapp

# Option C: Manual revert
pip install protobuf==3.17.0 grpcio==1.38.0
# Restart application
```

### Step 4: Verify Rollback

```bash
# Check versions
python3 -c "import google.protobuf; print(google.protobuf.__version__)"

# Run health checks
curl https://app.example.com/health

# Monitor for 30 minutes
```

### Step 5: Root Cause Analysis

- Document what went wrong
- Identify missed test case
- Create reproduction case
- Fix issue in development
- Retry migration

---

## Best Practices

### 1. Gradual Rollout

```python
# Use feature flags for gradual rollout
if feature_flag.is_enabled('protobuf_6'):
    client = etcd3.client()  # New version
else:
    client = legacy_etcd3.client()  # Old version
```

### 2. Monitoring

```python
# Add metrics for etcd operations
from prometheus_client import Counter, Histogram

etcd_operations = Counter(
    'etcd_operations_total',
    'Total etcd operations',
    ['operation', 'status']
)

etcd_latency = Histogram(
    'etcd_operation_duration_seconds',
    'Etcd operation latency'
)

@etcd_latency.time()
def etcd_put(key, value):
    try:
        client.put(key, value)
        etcd_operations.labels(operation='put', status='success').inc()
    except Exception as e:
        etcd_operations.labels(operation='put', status='error').inc()
        raise
```

### 3. Circuit Breaker

```python
from circuitbreaker import circuit

@circuit(failure_threshold=5, recovery_timeout=60)
def etcd_get(key):
    return client.get(key)
```

### 4. Health Checks

```python
def health_check():
    """Health check endpoint."""
    try:
        # Test etcd connection
        client = etcd3.client(timeout=2)
        client.put('health_check', 'ok')
        value, _ = client.get('health_check')
        client.delete('health_check')

        # Check protobuf version
        import google.protobuf
        proto_version = google.protobuf.__version__

        return {
            'status': 'healthy',
            'etcd': 'connected',
            'protobuf_version': proto_version
        }
    except Exception as e:
        return {
            'status': 'unhealthy',
            'error': str(e)
        }
```

---

## Timeline Example

### Week 1: Preparation & Development
- **Day 1-2:** Assessment, Python upgrade, dependency updates
- **Day 3-4:** Code changes, unit testing
- **Day 5:** Integration testing, CI/CD updates

### Week 2: Deployment
- **Day 6-7:** Staging deployment, load testing
- **Day 8:** Production deployment (20% traffic)
- **Day 9:** Production deployment (50% traffic)
- **Day 10:** Production deployment (100% traffic)

### Week 3: Monitoring
- **Day 11-15:** Monitor production, optimize performance
- **Day 16-17:** Documentation, team training

---

## Success Criteria

Migration is successful when:

- ✅ All tests passing on Python 3.9+
- ✅ Protobuf version is 6.33.3
- ✅ gRPC version is 1.76.0+
- ✅ No regression in functionality
- ✅ Performance metrics within acceptable range (±10%)
- ✅ No increase in error rates
- ✅ Production stable for 7 days
- ✅ Team trained on new version

---

## Support & Resources

### Documentation
- [Protobuf Upgrade Report](PROTOBUF_UPGRADE_REPORT.md)
- [Breaking Changes](BREAKING_CHANGES.md)
- [Protobuf Documentation](https://protobuf.dev/)

### Community
- python-etcd3 Issues: https://github.com/kragniz/python-etcd3/issues
- Stack Overflow: Tag `python-etcd3`, `protobuf`
- Protobuf Group: https://groups.google.com/g/protobuf

### Getting Help

If you encounter issues:
1. Check the [Troubleshooting](#troubleshooting) section
2. Search existing issues on GitHub
3. Create a new issue with:
   - Python version
   - protobuf version
   - Error message and stack trace
   - Minimal reproduction case

---

**Last Updated:** 2026-01-12
**Version:** 1.0
