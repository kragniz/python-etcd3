import sys

ignore_collect = ["setup.py"]
if sys.version_info[:2] < (3, 6):
    ignore_collect.append("tests/test_etcd3_aio.py")
