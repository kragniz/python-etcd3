import threading

import grpc

from six.moves import queue

import etcd3.etcdrpc as etcdrpc
import etcd3.events as events
import etcd3.utils as utils


class Watch(object):

    def __init__(self, watch_id, iterator=None, etcd_client=None):
        self.watch_id = watch_id
        self.etcd_client = etcd_client
        self.iterator = iterator

    def cancel(self):
        self.etcd_client.cancel_watch(self.watch_id)

    def iterator(self):
        if self.iterator is not None:
            return self.iterator
        else:
            raise


class Watcher(threading.Thread):

    def __init__(self, watchstub, timeout=None):
        threading.Thread.__init__(self)
        self.timeout = timeout
        self._watch_id_callbacks = {}
        self._watch_id_queue = queue.Queue()
        self._watch_id_lock = threading.Lock()
        self._watch_requests_queue = queue.Queue()
        self._watch_response_iterator = \
            watchstub.Watch(self._requests_iterator)
        self._callback = None
        self.daemon = True
        self.start()

    def run(self):
        try:
            for response in self._watch_response_iterator:
                if response.created:
                    self._watch_id_callbacks[response.watch_id] = \
                        self._callback
                    self._watch_id_queue.put(response.watch_id)

                callback = self._watch_id_callbacks.get(response.watch_id)
                if callback:
                    for event in response.events:
                        callback(events.new_event(event))
        except grpc.RpcError as e:
            self.stop()
            if self._watch_id_callbacks:
                for callback in self._watch_id_callbacks.values():
                    callback(e)

    @property
    def _requests_iterator(self):
        while True:
            request, self._callback = self._watch_requests_queue.get()
            if request is None:
                break
            yield request

    def add_callback(self, key, callback,
                     range_end=None,
                     start_revision=None,
                     progress_notify=False,
                     filters=None,
                     prev_kv=False):
        with self._watch_id_lock:
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
            self._watch_requests_queue.put((request, callback))
            return self._watch_id_queue.get(timeout=self.timeout)

    def cancel(self, watch_id):
        if watch_id is not None:
            self._watch_id_callbacks.pop(watch_id, None)
            cancel_watch = etcdrpc.WatchCancelRequest()
            cancel_watch.watch_id = watch_id
            request = etcdrpc.WatchRequest(cancel_request=cancel_watch)
            self._watch_requests_queue.put((request, None))

    def stop(self):
        self._watch_requests_queue.put((None, None))
