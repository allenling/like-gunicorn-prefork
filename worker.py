# coding=utf-8
from __future__ import unicode_literals
from __future__ import absolute_import
import os
import signal
import time
import sys


class Worker(object):

    def __init__(self):
        signal.signal(signal.SIGABRT, self.abort)

    def abort(self, signum, frame):
        print 'worker %s aborting' % os.getpid()
        sys.exit(1)

    def run(self):
        print 'worker %s runing' % os.getpid()
        while True:
            time.sleep(3)
