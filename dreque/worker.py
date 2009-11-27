
import logging
import socket
import time
from dreque.base import Dreque

class DrequeWorker(Dreque):
    def __init__(self, queues, server, db=None):
        self.queues = queues
        self.function_cache = {}
        super(DrequeWorker, self).__init__(server, db)
        self.log = logging.getLogger("dreque.worker")
        self.hostname = socket.gethostname()
        self.worker_id = "%s:%d"

    def work(self, interval=5):
        self.register_worker()

        try:
            while True:
                job = self.dequeue(self.queues)
                if not job:
                    if interval == 0:
                        break
                    time.sleep(interval)

                try:
                    self.working_on(job)
                    self.process(job)
                except Exception, exc:
                    self.log.info("Job failed (%s): %s" % (job, str(exc)))
                    # Requeue
                    queue = job.pop("queue")
                    self.push(queue, job)
                    self.failed()
                else:
                    self.done_working()

                if interval == 0:
                    break
        finally:
            self.unregister_worker()

    def process(self, job):
        func = self.lookup_function(job['func'])
        func(*job['args'], **job['kwargs'])

    def register_worker(self):
        self.redis.sadd(self._redis_key("workers"), self.worker_id)

    def unregister_worker(self):
        self.redis.srem(self._redis_key("workers"), self.worker_id)
        self.redis.delete(self._redis_key("worker:%s:started" % self.worker_id))
        self.stats.clear("processed:"+self.worker_id)
        self.stats.clear("failed:"+self.worker_id)

    def workers(self):
        return self.redis.smembers(self._redis_key("workers"))

    def working(self):
        workers = self.list_workers()
        if not workers:
            return []

        keys = [self._redis_key("worker:"+x) for x in workers]
        return dict((x, y) for x, y in zip(self.redis.mget(workers, keys)))

    def worker_exists(self, worker_id):
        return self.redis.sismember(self._redis_key("workers"), worker_id)

    def working_on(self, job):
        self.redis.set(self._redis_key("worker:"+self.worker_id),
            dict(
                queue = job['queue'],
                func = job['func'],
                args = job['args'],
                kwargs = job['kwargs'],
                run_at = time.time(),
            ))

    def done_working(self):
        self.processed()
        self.redis.delete(self._redis_key("worker:"+self.worker_id))

    def processed(self):
        self.stats.incr("processed")
        self.stats.incr("processed:" + self.worker_id)

    def failed(self):
        self.stats.incr("failed")
        self.stats.incr("failed:" + self.worker_id)

    def started(self):
        self.redis.set("worker:%s:started" % self.worker_id, time.time())

    def lookup_function(self, name):
        try:
            return self.function_cache[name]
        except KeyError:
            mod_name, func_name = name.rsplit('.', 1)
            mod = __import__(mod_name, {}, {}, [func_name])
            func = getattr(mod, func_name)
            self.function_cache[name] = func
        return func
