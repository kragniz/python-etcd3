class Etcd3Exception(Exception):
    pass


class KeyNotFoundError(Etcd3Exception):
    pass


class WatchTimedOut(Etcd3Exception):
    pass


class ConnectionTimedOut(Etcd3Exception):
    """connection timeout exception"""
    pass
