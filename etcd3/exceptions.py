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


class RevisionCompactedError(Etcd3Exception):
    def __init__(self, compacted_revision):
        self.compacted_revision = compacted_revision
        super(RevisionCompactedError, self).__init__()
