from etcd3.etcdrpc import rpc_pb2 as etcdrpc


class BaseCompare(object):
    def __init__(self, key):
        self.key = key
        self.value = None
        self.op = None

    def __eq__(self, other):
        self.value = other
        self.op = '='
        return self

    def __lt__(self, other):
        self.value = other
        self.op = '<'
        return self

    def __gt__(self, other):
        self.value = other
        self.op = '>'
        return self

    def __repr__(self):
        return "{}: {}('{}') {} '{}'".format(self.__class__, self.compare_type,
                                             self.key, self.op, self.value)

    def build_message(self):
        compare = etcdrpc.Compare()
        compare.key = self.key.encode('utf-8')

        if self.op == '=':
            compare.result = etcdrpc.Compare.EQUAL
        elif self.op == '<':
            compare.result = etcdrpc.Compare.LESS
        elif self.op == '>':
            compare.result = etcdrpc.Compare.GREATER
        else:
            raise  # TODO: add a proper exception class for this

        if self.compare_type == 'value':
            compare.target = etcdrpc.Compare.VALUE
            compare.value = self.value.encode('utf-8')
        elif self.compare_type == 'version':
            compare.target = etcdrpc.Compare.VERSION
            compare.version = int(self.value)
        elif self.compare_type == 'create':
            compare.target = etcdrpc.Compare.CREATE
            compare.create_revision = int(self.value)
        elif self.compare_type == 'mod':
            compare.target = etcdrpc.Compare.MOD
            compare.mod_revision = int(self.value)

        return compare


class Value(BaseCompare):
    compare_type = 'value'


class Version(BaseCompare):
    compare_type = 'version'


class Create(BaseCompare):
    compare_type = 'create'


class Mod(BaseCompare):
    compare_type = 'mod'


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
