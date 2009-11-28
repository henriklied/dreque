
try:
    import json
except ImportError:
    import simplejson as json
import logging
import time
from redis import Redis, ResponseError

from dreque.stats import StatsCollector

class Dreque(object):
    def __init__(self, server, db=None, key_prefix="dreque:"):
        self.log = logging.getLogger("dreque")

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

        self.key_prefix = key_prefix
        self.watched_queues = set()
        self.stats = StatsCollector(self.redis, self.key_prefix)

    # Low level

    def push(self, queue, item, delay=None):
        self.watch_queue(queue)

        if delay:
            if delay < 31536000:
                delay = int(delay + time.time())
            # TODO: In Redis>=1.1 can use an ordered set: zadd(delayed, delay, encoded_item)
            self.redis.push(self._delayed_key(queue), "%.12x:%s" % (delay, self.encode(item)))
        else:
            self.redis.push(self._queue_key(queue), self.encode(item))

    def check_delayed(self, queue, num=10):
        """Check for available jobs in the delayed queue and move them to the live queue"""
        # TODO: In Redis>=1.1 can use an ordered set: zrangebyscore(delayed, 0, current_time)
        delayed_key = self._delayed_key(queue)
        queue_key = self._queue_key(queue)
        try:
            jobs = self.redis.sort(delayed_key, num=num, alpha=True)
        except ResponseError, exc:
            if str(exc) != "no such key":
                raise
            return
        now = time.time()
        for j in jobs:
            available, encoded_job = j.split(':', 1)
            available = int(available, 16)
            if available < now:
                if self.redis.lrem(delayed_key, j) > 0:
                    # Only copy the job if it still exists.. nobody else got to it first
                    self.redis.push(queue_key, encoded_job)

    def pop(self, queue):
        self.check_delayed(queue)
        msg = self.redis.pop(self._queue_key(queue))
        return self.decode(msg) if msg else None

    def poppush(self, source_queue, dest_queue):
        msg = self.redis.poppush(self._queue_key(source_queue), self._queue_key(dest_queue))
        return self.decode(msg) if msg else None

    def size(self, queue):
        return self.redis.llen(self._queue_key(queue))

    def peek(self, queue, start=0, count=1):
        return self.list_range(self._queue_key(queue), start, count)

    def list_range(self, key, start=0, count=1):
        if count == 1:
            return self.decode(self.redis.lindex(key, start))
        else:
            return [self.decode(x) for x in self.redis.lrange(key, start, start+count-1)]

    # High level

    def enqueue(self, queue, func, *args, **kwargs):
        delay = kwargs.pop('_delay', None)
        if not isinstance(func, basestring):
            func = "%s.%s" % (func.__module__, func.__name__)
        self.push(queue, dict(func=func, args=args, kwargs=kwargs), delay)

    def dequeue(self, queues, worker_queue=None):
        now = time.time()
        for q in queues:
            if worker_queue:
                msg = self.redis.poppush(self._queue_key(source_queue), self._redis_key(dest_queue))
                if msg:
                    msg = self.decode(msg)
            else:
                msg = self.pop(q)
            if msg:
                msg['queue'] = q
                return msg
 
    # Queue methods

    def queues(self):
        return self.redis.smembers(self._queue_set_key())

    def remove_queue(self, queue):
        self.watched_queues.discard(queue)
        self.redis.srem(self._queue_set_key(), queue)
        self.redis.delete(self._queue_key(queue))
        self.redis.delete(self._delayed_key(queue))

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

    def _delayed_key(self, queue):
        return self._redis_key("delayed:" + queue)

    def _redis_key(self, key):
        return self.key_prefix + key
