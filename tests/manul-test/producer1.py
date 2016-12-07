# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.append(dirname(dirname(dirname(dirname(abspath(__file__))))))

import sqlite3
import time
from taskqueue import TaskQueue

taskpool = '/tmp/test-taskpool.sqlite3'


if __name__ == '__main__':
    q = TaskQueue(taskpool, 1)
    i = 0
    while True:
        i += 1
        q.put(('GC-A{}'.format(i), 'GC-B{}'.format(i+1), ))
        time.sleep(5)
        