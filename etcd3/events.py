class Event(object):

    def __init__(self, event):
        self.key = event.kv.key
        self.__event = event

    def __getattr__(self, name):
        if name.startswith('prev_'):
            return getattr(self.__event.prev_kv, name[5:])
        return getattr(self.__event.kv, name)

    def __str__(self):
        return '{type} key={key} value={value}'.format(type=self.__class__,
                                                       key=self.key,
                                                       value=self.value)


class PutEvent(Event):
    pass


class DeleteEvent(Event):
    pass


__events_impl = {sc.__name__: sc for sc in Event.__subclasses__()}


def new_event(event):
    class_name = event.EventType.Name(event.type).upper()
    class_name = class_name[0] + class_name[1:].lower() + 'Event'
    return __events_impl[class_name](event)
