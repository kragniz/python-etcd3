import threading

import grpc

from six.moves import queue

import etcd3.etcdrpc as etcdrpc
import etcd3.events as events
import etcd3.utils as utils


class Watcher(threading.Thread):

    def __init__(self, watchstub):
        threading.Thread.__init__(self)
        self.__watch_id_callbacks = {}
        self.__watch_id_queue = queue.Queue()
        self.__watch_id_lock = threading.Lock()
        self.__watch_requests_queue = queue.Queue()
        self.__watch_response_iterator = \
            watchstub.Watch(self.__requests_iterator)
        self.__callback = None
        self.daemon = True
        self.start()

    def run(self):
        try:
            for response in self.__watch_response_iterator:
                if response.created and self.__callback:
                    self.__watch_id_callbacks[response.watch_id] = \
                        self.__callback
                    self.__watch_id_queue.put(response.watch_id)

                callback = self.__watch_id_callbacks.get(response.watch_id)
                if callback:
                    for event in response.events:
                        callback(events.new_event(event))
        except grpc.RpcError:
            self.cancel()

    @property
    def __requests_iterator(self):
        while True:
            request, self.__callback = self.__watch_requests_queue.get()
            if request is None:
                break
            yield request

    def add_callback(self, key, callback,
                     range_end=None,
                     start_revision=None,
                     progress_notify=False,
                     filters=None,
                     prev_kv=False):
        if callback:
            self.__watch_id_lock.acquire()
        create_watch = etcdrpc.WatchCreateRequest()
        create_watch.key = utils.to_bytes(key)
        if range_end is not None:
            create_watch.range_end = utils.to_bytes(range_end)
        if start_revision is not None:
            create_watch.start_revision = start_revision
        if progress_notify:
            create_watch.progress_notify = progress_notify
        if filters is not None:
            create_watch.filters = filters
        if prev_kv:
            create_watch.prev_kv = prev_kv
        request = etcdrpc.WatchRequest(create_request=create_watch)
        self.__watch_requests_queue.put((request, callback))
        if callback:
            watch_id = self.__watch_id_queue.get()
            self.__watch_id_lock.release()
            return watch_id

    def cancel(self, watch_id=None):
        if watch_id is None:
            request = None
        else:
            self.__watch_id_callbacks.pop(watch_id, None)
            cancel_watch = etcdrpc.WatchCancelRequest()
            cancel_watch.watch_id = watch_id
            request = etcdrpc.WatchRequest(cancel_request=cancel_watch)
        self.__watch_requests_queue.put((request, None))
