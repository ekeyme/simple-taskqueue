# -*- coding: utf-8 -*-
import sys
from os.path import abspath, dirname
sys.path.append('/home/mozz/')

import sqlite3
from taskqueue import TaskQueue
from taskqueue.helper import setup_taskqueue_tables

taskpool = '/tmp/test-taskpool.sqlite3'

def add_taskqueue(taskpool):
    db = sqlite3.connect(taskpool)
    cur = db.cursor()
    cur.execute("""INSERT INTO tasklock(locked, desc, update_time) 
                       VALUES(0, 'a new test lock', datetime('now', 'localtime'))
                    """)
    lockid = cur.lastrowid
    assert lockid
    cur.execute("""INSERT INTO taskqueue(lockid, qlocked, desc, update_time) 
                       VALUES(?, 0, 'a test task', datetime('now', 'localtime'))""", 
                    (lockid, ))
    taskid = cur.lastrowid
    assert lockid
    db.commit()
    db.close()

    q = TaskQueue(taskpool, taskid)
    q.put(('GC-A0001', 'GC-A0002', 'GC-A0003', 'GC-A0004', 'GC-A0005', ))

setup_taskqueue_tables(taskpool)
add_taskqueue(taskpool)

