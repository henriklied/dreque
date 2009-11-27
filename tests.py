
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
        while self.dreque.pop(self.queue):
            pass

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

    def testWorker(self):
        import tests
        self.dreque.enqueue("test", tests.set_something, "worker_test")

        worker = DrequeWorker(["test"], "127.0.0.1")
        worker.work(0)

        self.failUnlessEqual(tests.something, "worker_test")

if __name__ == '__main__':
    unittest.main()
