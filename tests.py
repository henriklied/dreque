
import time
import unittest
from dreque import Dreque, DrequeWorker

something = None
def set_something(val):
    global something
    something = val

class TestDreque(unittest.TestCase):
    def setUp(self):
        import logging
        logging.basicConfig(level=logging.DEBUG)
        self.dreque = Dreque("127.0.0.1")
        self.queue = "test"
        self.dreque.remove_queue(self.queue)

    def tearDown(self):
        pass

    def testSimple(self):
        self.dreque.push("test", "foo")
        self.failUnlessEqual(self.dreque.pop("test"), "foo")
        self.failUnlessEqual(self.dreque.pop("test"), None)

    def testFunction(self):
        import tests
        self.dreque.enqueue("test", tests.set_something, "positional")
        self.failUnlessEqual(self.dreque.dequeue(["test"]), dict(queue="test", func="tests.set_something", args=["positional"], kwargs={}))
        self.dreque.enqueue("test", tests.set_something, keyword="argument")
        self.failUnlessEqual(self.dreque.dequeue(["test"]), dict(queue="test", func="tests.set_something", args=[], kwargs={'keyword':"argument"}))

    def testPositionalWorker(self):
        import tests
        self.dreque.enqueue("test", tests.set_something, "worker_test")
        worker = DrequeWorker(["test"], "127.0.0.1", nofork=True)
        worker.work(0)
        self.failUnlessEqual(tests.something, "worker_test")

    def testKeywordWorker(self):
        import tests
        self.dreque.enqueue("test", tests.set_something, val="worker_test")
        worker = DrequeWorker(["test"], "127.0.0.1", nofork=True)
        worker.work(0)
        self.failUnlessEqual(tests.something, "worker_test")

    def testDelayedJob(self):
        import tests
        self.dreque.enqueue("test", tests.set_something, val="worker_test", _delay=1)
        self.failUnlessEqual(self.dreque.dequeue("test"), None)
        time.sleep(1.5)
        self.failUnlessEqual(self.dreque.dequeue(["test"]), dict(queue="test", func="tests.set_something", args=[], kwargs={'val':"worker_test"}))

if __name__ == '__main__':
    unittest.main()
