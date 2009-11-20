
import json
import time
from redis import Redis

from dreque.stats import StatsCollector

class Dreque(object):
    def __init__(self, server, db=None):
        if isinstance(server, (tuple, list)):
            host, port = server
            self.redis = Redis(server[0], server[1], db=db)
        elif isinstance(server, basestring):
            host = server
            port = None
            if ':' in server:
                host, port = server.split(':')
            self.redis = Redis(host, port, db=db)
        else:
            self.redis = server
        self.watched_queues = set()
        self.stats = StatsCollector(self.redis, "dreque")

    # Low level

    def push(self, queue, item):
        self.watch_queue(queue)
        self.redis.push(self._queue_key(queue), self.encode(item))

    def pop(self, queue):
        return self.decode(self.redis.pop(self._queue_key(queue)))

    def poppush(self, source_queue, dest_queue):
        msg = self.redis.poppush(self._queue_key(source_queue), self._queue_key(dest_queue))
        return self.decode(msg)

    def size(self, queue):
        return self.redis.llen(self._queue_key(queue))

    def peek(self, queue, start=0, count=1):
        return self.list_range(self._queue_key(queue), start, count)

    def list_range(self, key, start=0, count=1):
        if count == 1:
            return self.decode(self.redis.lindex(key, start))
        else:
            return [self.decode(x) for x in self.redis.lrange(key, start, start+count-1)]

    # Workers

    def grab_one(self, queues, worker_queue=None, grab_for=60*60):
        now = time.time()
        expires = now + grab_for
        for q in queues:
            if worker_queue:
                msg = self.poppush(q, worker_queue)
            else:
                msg = self.pop(q)
            if msg:
                return msg

    # Queue level

    def queues(self):
        return self.redis.smembers(self._queue_set_key())

    def remove_queue(self, queue):
        self.watched_queues.discard(queue)
        self.redis.srem(self._queue_set_key(), queue)
        self.redis.delete(self._queue_key(queue))

    def watch_queue(self, queue):
        if queue not in self.watched_queues:
            self.watched_queues.add(queue)
            self.redis.sadd(self._queue_set_key(), queue)

    #

    def encode(self, value):
        return json.dumps(value)

    def decode(self, value):
        return json.loads(value)

    def _queue_key(self, queue):
        return self._redis_key("queue:" + queue)

    def _queue_set_key(self):
        return self._redis_key("queues")

    def _redis_key(self, key):
        return "deque:" + key
