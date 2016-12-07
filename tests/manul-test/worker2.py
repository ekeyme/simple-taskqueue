# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.append(dirname(dirname(dirname(dirname(abspath(__file__))))))

import sqlite3
import time
from taskqueue import TaskQueue


taskpool = '/tmp/test-taskpool.sqlite3'

def do_something(item):
    print(item)
    time.sleep(10)
    if int(item[-1:]) % 2 == 0:
        return True
    return False

if __name__ == '__main__':
    q = TaskQueue(taskpool, 1)
    tasklock = q.tasklock()
    if tasklock.acquire():
        items = q.get()
        tracing = q.tasktracing(items)
        for i in items:
            if do_something(i):
                tracing.ok(i)
            else:
                tracing.fail(i)
        tasklock.release()
    else:
        print("exit with no lock")
