
from dreque.base import Dreque

class DrequeWorker(Dreque):
    def __init__(self, *args, **kwargs):
        self.queues = kwargs.pop('queues')

    def list_workers(self):
        return self.redis.smembers(self._redis_key("workers"))
