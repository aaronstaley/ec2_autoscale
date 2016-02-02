import logging
import threading

from autoscale.threadpool import ThreadPool

def main():
    """
    Test code.
    """

    logging.basicConfig(level=logging.DEBUG)

    import time, random

    def hi(x):
        time.sleep(random.random())
        print 'hi %s' % x

    def test(pool):
        time.sleep(0.3)
        print 'pool events', pool.events

    def inf():
        while True:
            print 'inf loop'
            for _ in xrange(50):
                time.sleep(0.01)

    p = ThreadPool(daemon = False)
    jids = []
    for i in range(10):
        jid = p.add_task(target=hi, args = (i,))
        jids.append(jid)

    print 'threads',  len(p.threads), p.threads
    print 'jobs in progress', p.jobs_in_progress
    print 'events', p.events

    print 'jids', jids

    t = threading.Thread(target=test, args=(p,))
    t.start()

    p.join(jids)
    time.sleep(1.0)
    print 'jobs in progress', p.jobs_in_progress
    print 'events', p.events

    jid = p.add_task(target = inf)
    time.sleep(0.5)
    print 'infinite job on thread', p.jobs_in_progress.get(jid)
    retval = p.join(jid, timeout = 1.0)
    print 'result of join', retval
    killed = p.kill(jid)
    print 'killed job?', killed
    retval = p.join(jid)
    print 'result of second join', retval

    time.sleep(1.0)
    p.add_task(target = hi, args = (23,))
    time.sleep(0.2)
    print 'jobs in progress', p.jobs_in_progress
    print 'events', p.events
    print 'threads',  len(p.threads), p.threads

    p.terminate()

if __name__ == '__main__':
    main()