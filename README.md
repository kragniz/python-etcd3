# etcd client for asyncio

This project is based on based on based on [grpclib](https://github.com/vmagamedov/grpclib) and requires **Python >=3.8**.

* Free software: Apache Software License 2.0
* Documentation: https://etcd3aio.readthedocs.io.

## Basic usage:

```python
import etcd3

etcd = etcd3.client()
await etcd.get('foo')
await etcd.put('bar', 'doot')
await etcd.delete('bar')

# locks
lock = etcd.lock('thing')
await lock.acquire()
# do something
await lock.release()

async with etcd.lock('doot-machine') as lock:
    # do something

# transactions
await etcd.transaction(
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
events_iterator, cancel = await etcd.watch("/doot/watch")
async for event in events_iterator:
    print(event)
    watch_count += 1
    if watch_count > 10:
        await cancel()

# watch prefix
watch_count = 0
events_iterator, cancel = await etcd.watch_prefix("/doot/watch/prefix/")
async for event in events_iterator:
    print(event)
    watch_count += 1
    if watch_count > 10:
        await cancel()

# receive watch events via callback function
def watch_callback(event):
    print(event)

watch_id = await etcd.add_watch_callback("/anotherkey", watch_callback)

# cancel watch
await etcd.cancel_watch(watch_id)

# receive watch events for a prefix via callback function
def watch_callback(event):
    print(event)
```

## Thanks

This project is a fork of amazing work of (python-etcd3)[https://github.com/kragniz/python-etcd3]
