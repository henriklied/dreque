
Dreque
======

Dreque is a persistent job queue that uses Redis for storage.
It is basically a Python version of Resque (http://github.com/defunkt/resque).

License
-------

BSD License

See 'LICENSE' for details.

Usage
-----

Submitting jobs:

    from dreque import Dreque

    def some_job(argument):
        pass

    dreque = Dreque("127.0.0.1")
    dreque.enqueue("queue", some_job, argument="foo")

Worker:

    from dreque import DrequeWorker

    worker = DrequeWorker(["queue"], "127.0.0.1")
    worker.work()
