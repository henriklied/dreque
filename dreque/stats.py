
class StatsCollector(object):
    def __init__(self, store, prefix=''):
        self.store = store
        self.prefix = prefix

    def incr(self, key, delta=1):
        key = self._key(key)
        try:
            return self.store.incr(key, delta)
        except ValueError:
            if not self.store.add(key, 1):
                # Someone set the value before us
                self.store.incr(key, delta)

    def decr(self, key, delta=1):
        key = self._key(key)
        try:
            return self.store.decr(key, delta)
        except ValueError:
            if not self.store.add(key, 0):
                # Someone set the value before us
                self.store.decr(key, delta)

    def get(self, key):
        return self.store.get(self._key(key))

    def clear(self, key):
        self.store.delete(self._key(key))

    def _key(self, key):
        return "%s:stat:%s" % (self.prefix, key)
