class Etcd3Exception(Exception):
    pass


class WatchTimedOut(Etcd3Exception):
    pass


class InternalServerError(Etcd3Exception):
    pass


class ConnectionFailedError(Etcd3Exception):
    pass


class ConnectionTimeoutError(Etcd3Exception):
    pass


class PreconditionFailedError(Etcd3Exception):
    pass
