# coding=utf-8
from __future__ import unicode_literals
from __future__ import absolute_import
import os
import signal
import time
import sys


class Worker(object):

    def __init__(self):
        self.alive = True
        signal.signal(signal.SIGTERM, self.sigterm)

    def sigterm(self, signum, frame):
        '''
        sigterm means we should gracefully shutdown
        '''
        print 'worker %s terming with %s' % (os.getpid(), signum)
        self.alive = False

    def run(self):
        print 'worker %s runing' % os.getpid()
        while self.alive:
            time.sleep(3)
