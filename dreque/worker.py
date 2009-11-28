
import os
import logging
import socket
import time
from dreque.base import Dreque
from dreque.utils import setprocname

class DrequeWorker(Dreque):
    def __init__(self, queues, server, db=None):
        self.queues = queues
        self.function_cache = {}
        super(DrequeWorker, self).__init__(server, db)
        self.log = logging.getLogger("dreque.worker")
        self.hostname = socket.gethostname()
        self.pid = os.getpid()
        self.worker_id = "%s:%d" % (self.hostname, self.pid)

    def work(self, interval=5):
        self.register_worker()

        try:
            while True:
                job = self.dequeue(self.queues)
                if not job:
                    if interval == 0:
                        break
                    time.sleep(interval)
                    continue

                try:
                    self.working_on(job)
                    self.process(job)
                except Exception, exc:
                    import traceback
                    self.log.info("Job failed (%s): %s\n%s" % (job, str(exc), traceback.format_exc()))
                    # Requeue
                    queue = job.pop("queue")
                    if 'fails' not in job:
                        job['fails'] = 1
                    else:
                        job['fails'] += 1
                    if job['fails'] < 5:
                        self.push(queue, job)
                    self.failed()
                else:
                    self.done_working()

                if interval == 0:
                    break
        finally:
            self.unregister_worker()

    def process(self, job):
        from multiprocessing import Process

        p = Process(target=self.dispatch, args=(job,))
        p.start()
        setprocname("dreque: Forked %d at %d" % (p.pid, time.time()))
        p.join()

        if p.exitcode != 0:
            raise Exception("Job failed")

    def dispatch(self, job):
        setprocname("dreque: Processing %s since %d" % (job['queue'], time.time()))
        func = self.lookup_function(job['func'])
        kwargs = dict((str(k), v) for k, v in job['kwargs'].items())
        func(*job['args'], **kwargs)

    def register_worker(self):
        self.redis.sadd(self._redis_key("workers"), self.worker_id)

    def unregister_worker(self):
        self.redis.srem(self._redis_key("workers"), self.worker_id)
        self.redis.delete(self._redis_key("worker:%s:started" % self.worker_id))
        self.stats.clear("processed:"+self.worker_id)
        self.stats.clear("failed:"+self.worker_id)

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
            mod = __import__(str(mod_name), {}, {}, [str(func_name)])
            func = getattr(mod, func_name)
            self.function_cache[name] = func
        return func

    #

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
