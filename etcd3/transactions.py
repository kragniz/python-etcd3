from etcd3.etcdrpc import rpc_pb2 as etcdrpc


class BaseCompare(object):
    def __init__(self, key):
        self.key = key
        self.value = None
        self.op = None

    # TODO check other is of correct type for compare
    # Version, Mod and Create can only be ints
    def __eq__(self, other):
        self.value = other
        self.op = etcdrpc.Compare.EQUAL
        return self

    def __lt__(self, other):
        self.value = other
        self.op = etcdrpc.Compare.LESS
        return self

    def __gt__(self, other):
        self.value = other
        self.op = etcdrpc.Compare.GREATER
        return self

    def __repr__(self):
        return "{}: {}('{}') {} '{}'".format(self.__class__, self.compare_type,
                                             self.key, self.op, self.value)

    def build_message(self):
        compare = etcdrpc.Compare()
        compare.key = self.key.encode('utf-8')

        if self.op is None:
            raise ValueError('op must be one of =, < or >')

        compare.result = self.op

        self.build_compare(compare)
        return compare


class Value(BaseCompare):
    def build_compare(self, compare):
        compare.target = etcdrpc.Compare.VALUE
        compare.value = self.value.encode('utf-8')


class Version(BaseCompare):
    def build_compare(self, compare):
        compare.target = etcdrpc.Compare.VERSION
        compare.version = int(self.value)


class Create(BaseCompare):
    def build_compare(self, compare):
        compare.target = etcdrpc.Compare.CREATE
        compare.create_revision = int(self.value)


class Mod(BaseCompare):
    def build_compare(self, compare):
        compare.target = etcdrpc.Compare.MOD
        compare.mod_revision = int(self.value)


class Put(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value


class Get(object):
    def __init__(self, key):
        self.key = key


class Delete(object):
    def __init__(self, key):
        self.key = key
