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

class RoutineTest(unittest.TestCase):
    
    def setUp(self):
        import sqlite3
        import tempfile
        from taskqueue.helper import setup_taskqueue_tables

        self.dbf = tempfile.NamedTemporaryFile(delete=True).name
        setup_taskqueue_tables(self.dbf)
        self.db = db = sqlite3.connect(self.dbf)
        cur = db.cursor()
        #new lock
        cur.execute("""INSERT INTO tasking_lock(locked, desc, update_time) 
                       VALUES(0, 'a new lock', datetime('now', 'localtime'))
                    """)
        self.lockid = cur.lastrowid
        assert self.lockid, "cannot create a new lock."
        # new tast info
        cur.execute("""INSERT INTO tasks(lockid, desc) VALUES(?, 'test task')""", (self.lockid, ))
        self.taskid = cur.lastrowid
        assert self.taskid, "cannot create a new task."
        db.commit()

    def tearDown(self):
        import os
        os.unlink(self.dbf)

    def test_taskqueue_get(self):
        pass


if __name__ == '__main__':
    unittest.main()
