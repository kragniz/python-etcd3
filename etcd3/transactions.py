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



class Value(BaseCompare):
    compare_type = 'value'


class Version(BaseCompare):
    compare_type = 'version'


class Put(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value
