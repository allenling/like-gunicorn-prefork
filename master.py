# coding=utf-8
from __future__ import unicode_literals
from __future__ import absolute_import
import os
import time
import signal
import errno
from worker import Worker


class Master(object):

    def __init__(self, worker_number):
        self.worker_number = worker_number
        self.SIGNALS = []
        self.workers = {}

    def start(self):
        # initial resources, like create connection vers
        pass

    def run(self):
        # run will loop to monitor workers
        self.start()
        self.init_signals()
        self.manage_workers()
        while True:
            time.sleep(10)
            # monitor workers
            self.manage_workers()

    def init_signals(self):
        '''
        signal handler
        '''
        signal.signal(signal.SIGCHLD, self.sigchild)

    def manage_workers(self):
        '''
        increase/decrease workers
        '''
        if len(self.workers) < self.worker_number:
            self.spawn_workers()
        elif len(self.workers) > self.worker_number:
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

    def sigchild(self, signum, frame):
        '''
        When a worker process dead, kernel will send SIGCHD to parent process, and parent process should call wait/waitpid to term
        worker process which is dead completely to avoid zombie process
        And then, we should call manage_workers to start a new worker process if there is a dead worker process indeed
        Like gunicorn, just wake up master by writing a pipe to manager_workers or call manager_workers directly
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
        self.manage_workers()

if __name__ == '__main__':
    master = Master(2)
    master.run()
