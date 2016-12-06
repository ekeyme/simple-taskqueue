#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit test for ffaheader

"""


import unittest

import sys
from os.path import abspath, dirname
sys.path.append(dirname(dirname(dirname(abspath(__file__)))))

import taskqueue
from taskqueue import TaskQueue

class RoutineTest(unittest.TestCase):
    
    def setUp(self):
        import sqlite3
        import tempfile
        import json
        from taskqueue.helper import setup_taskqueue_tables

        self.dbf = tempfile.NamedTemporaryFile(delete=True).name
        setup_taskqueue_tables(self.dbf)
        self.db = db = sqlite3.connect(self.dbf)
        self.items = ['GC-A0001', 'GC-A0002', 'GC-A0003']
        cur = db.cursor()
        #new lockqueue
        cur.execute("""INSERT INTO tasklock(locked, desc, update_time) 
                       VALUES(0, 'a new test lock', datetime('now', 'localtime'))
                    """)
        self.lockid = cur.lastrowid
        assert self.lockid, "cannot create a new lock."
        # new tast info
        s = json.dumps(self.items)
        cur.execute("""INSERT INTO taskqueue(lockid, items, qlocked, desc, update_time) 
                       VALUES(?, ?, 0, 'a test task', datetime('now', 'localtime'))""", 
                    (self.lockid, s))
        self.taskid = cur.lastrowid
        assert self.taskid, "cannot create a new task."
        db.commit()

    def tearDown(self):
        import os
        os.unlink(self.dbf)

    def test_taskqueue_get1(self):
        """TaskQueue.get should raise QueueLockTimeOut if queue_locked is always locked"""

        # make queque lock locked
        self.db.execute("""UPDATE taskqueue SET qlocked=1 WHERE taskid=?""", (self.taskid, ))
        self.db.commit()

        q = TaskQueue(self.dbf, self.taskid)
        with self.assertRaises(taskqueue.QueueLockTimeOut):
            q.get()

    def test_taskqueue_get2(self):
        """TaskQueue.get should return right result"""

        q = TaskQueue(self.dbf, self.taskid)
        # get the previous items
        self.assertEqual(q.get(), self.items)
        # get without items
        self.assertEqual(q.get(), [])
        # put and get by self
        items = ('GC-A0004', 'GC-A0005')
        q.put(items)
        self.assertEqual(q.get(), list(items))
        # put by other and get
        q2 = TaskQueue(self.dbf, self.taskid)
        items = ['GC-A0006', 'GC-A0007']
        q2.put(items)
        self.assertEqual(q.get(), items)

    def test_tasklock1(self):
        """tasklock should Return False when lock is locked"""

        # queue No.1 lock the tasklock without release
        q1 = TaskQueue(self.dbf, self.taskid)
        tasklock1 = q1.tasklock()
        r = tasklock1.acquire()
        self.assertEqual(r, True)

        # now queue No.2 acquire the tasklock
        q2 =  TaskQueue(self.dbf, self.taskid)
        tasklock2 = q2.tasklock()
        self.assertEqual(tasklock2.acquire(), False)

    def test_tasklock2(self):
        """tasklock should work right with other"""

        # queue No.1 tasklock without release
        q1 = TaskQueue(self.dbf, self.taskid)
        tasklock1 = q1.tasklock()
        r = tasklock1.acquire()
        self.assertEqual(r, True)
        tasklock1.release()

        # now queue No.2 acquire the tasklock
        q2 =  TaskQueue(self.dbf, self.taskid)
        tasklock2 = q2.tasklock()
        self.assertEqual(tasklock2.acquire(), True)
        tasklock2.release()

    def test_tasktracing(self):
        q = TaskQueue(self.dbf, self.taskid)
        assert q.tasklock().acquire() is True
        items = q.get()
        tracing = q.tasktracing(items)
        for i in range(len(items)):
            if i % 2 == 0:
                tracing.ok(i)
            else:
                tracing.fail(i)
        r = self.db.execute("""SELECT tracing FROM tasktracing 
                               WHERE taskid=?""", 
                            (self.taskid, )).fetchone()[0]
        self.assertEqual(r, '0:ok,1:fail,2:ok')


if __name__ == '__main__':
    unittest.main()
