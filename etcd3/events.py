class Event(object):
    def __init__(self, key, value, version):
        self.key = key
        self.value = value
        self.version = version
        self.type = 0

    def __str__(self):
        return '{type} key={key} value={value}'.format(type=self.__class__,
                                                       key=self.key,
                                                       value=self.value)


class PutEvent(Event):
    def __init__(self, *args):
        super(PutEvent, self).__init__(*args)
        self.type = 'put'


class DeleteEvent(Event):
    def __init__(self, *args):
        super(PutEvent, self).__init__(*args)
        self.type = 'delete'
