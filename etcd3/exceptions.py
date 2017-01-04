class Etcd3Exception(Exception):
    pass


class KeyNotFoundError(Etcd3Exception):
    pass


class WatchTimedOut(Etcd3Exception):
    pass


class InternalServerErrorException(Etcd3Exception):
    pass


class ConnectionFailedException(Etcd3Exception):
    pass


class ConnectionTimeoutException(Etcd3Exception):
    pass
