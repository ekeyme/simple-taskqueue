# -*- coding: utf-8 -*-
"""
Task tracing module

This module is for tracing the working status of each item in taskqueue.
It is optional.


Class:

TaskTracing


"""

import json


class TaskTracing(object):
    """For tracing task doing
    
    Args:

    tracedb - taskpool database connection

    taskid - taskid in taskqueue table

    items - a list of task items to be finish


    Methods:

    def ok(self, markname)
        Update the tracing column in tasktracing table, indicate this 
        markname item is done ok.

    def fail(self, markname)
        Update the tracing column in tasktracing table, indicate this 
        markname item is done fail.

    """

    def __init__(self, tracedb, taskid, items):
        if not items:
            raise KeyError('empty items.')
        self.db = tracedb
        self.taskid = taskid
        self.cur = cur = self.db.cursor()
        cur.execute("""INSERT INTO tasktracing(taskid, start_time, items) 
                       VALUES(?, datetime('now', 'localtime'), ?)""", 
                    (self.taskid, json.dumps(items)))
        self.db.commit()
        self.items = items
        self.id = cur.lastrowid
        self.tracing_items = []

    def ok(self, markname):
        """Indicates the item is done ok"""

        self._tracing('{}:ok'.format(markname))

    def fail(self, markname):
        """Indicates the item is done fail"""

        self._tracing('{}:fail'.format(markname))

    def _tracing(self, s):
        """Upate tracing table"""

        self.tracing_items.append(s)
        self.cur.execute("""UPDATE tasktracing SET tracing=? WHERE id=?""", 
                         (','.join(self.tracing_items), self.id))
        self.db.commit()
