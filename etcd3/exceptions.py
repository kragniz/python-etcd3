class Etcd3Exception(Exception):
    pass


class WatchTimedOut(Etcd3Exception):
    pass
