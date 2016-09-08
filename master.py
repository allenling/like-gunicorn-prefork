# coding=utf-8
from __future__ import unicode_literals
from __future__ import absolute_import
from collections import deque
import os
import time
import signal
import errno
from worker import Worker


class Master(object):

    def __init__(self, worker_number):
        self.worker_number = worker_number
        self.SIGNALS = {}
        for i in "HUP QUIT INT TERM TTIN TTOU CHLD".split():
            sig = getattr(signal, "SIG%s" % i)
            self.SIGNALS[sig] = ('SIG%s' % i).lower()
        '''
        Master signal list, we deal with signals serially
        If process signal asynchronously, processing a term signal while a ttin signal still be processing, that is unexpected
        '''
        self.signals = deque([])
        self.workers = {}

    def start(self):
        # initial resources, like create a connection vers
        pass

    def run(self):
        # run will loop to monitor workers
        self.start()
        self.init_signals()
        self.manage_workers()
        while True:
            # simply sleep, no select on pip
            time.sleep(1)
            # process signals serially
            sig = self.signals.popleft() if self.signals else None
            if sig is None:
                # kill timeout worker
                self.checkout_timeout_worker()
                # monitor workers
                self.manage_workers()
                continue
            # handle signal
            if sig not in self.SIGNALS:
                print 'unsupport signal %s' % sig
                continue
            sig_name = self.SIGNALS.get(sig)
            sig_method = getattr(self, sig_name, None)
            if sig_method is None:
                print 'this is no any method to handle signal %s' % sig_name
                continue
            sig_method()

    def handle_signal(self, sig_number, frame):
        self.signals.append(sig_number)

    def checkout_timeout_worker(self):
        '''
        check if this is any worker doing something too long and kill it
        '''
        pass

    def init_signals(self):
        '''
        signal handler
        '''
        for sig in self.SIGNALS:
            signal.signal(sig, self.handle_signal)
        # we would not deal with SIGCHLD in a difference way, just deal with it like other signals
        # signal.signal(signal.SIGCHLD, self.sigchild)

    def manage_workers(self):
        '''
        increase/decrease workers
        '''
        if len(self.workers) < self.worker_number:
            print 'spawn workers'
            self.spawn_workers()
        elif len(self.workers) > self.worker_number:
            print 'kill extra workers'
            self.kill_workers()

    def spawn_workers(self):
        '''
        create workers
        '''
        for _ in range(self.worker_number - len(self.workers)):
            worker_object = Worker()
            pid = os.fork()
            if pid != 0:
                self.workers[pid] = worker_object
            else:
                worker_object.run()

    def kill_workers(self):
        '''
        kill workers
        '''
        pass

    def kill_worker(self, pid):
        '''
        kill a single worker
        '''
        pass

    def sigchld(self):
        '''
        When a worker process dead, kernel will send SIGCHD to parent process, and parent process should call wait/waitpid to term
        worker process which is dead completely to avoid zombie process

        And then, we should call manage_workers to start a new worker process if there is a dead worker process indeed
        Like gunicorn, just wake up master by writing a pipe to manager_workers or call manager_workers directly

        Or we just do noting, cause next loop in run method would call manage_workers
        '''
        try:
            while True:
                # -1 means all child process, os.WNOHANG means do not be blocked, if there is not any child process dead, just return
                chd_pid, exit_code = os.waitpid(-1, os.WNOHANG)
                if not chd_pid:
                    break
                print 'child %s exit, exit_code: %s' % (chd_pid, exit_code >> 8)
                self.workers.pop(chd_pid)
        except OSError as e:
            if e.error != errno.ECHILD:
                raise
        # self.manage_workers()

if __name__ == '__main__':
    master = Master(2)
    master.run()
