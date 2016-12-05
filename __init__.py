# -*- coding: utf-8 -*-
"""
Simple taskqueue

"""

import sqlite3


class TaskQueue(object):
    def __init__(self, taskpool, taskid):
        """
        Args:
            taskpool - an sqlite3 database path to store the task information
            taskid - task id

        """

        self.db = sqlite3.connect(taskpool)
        self.taskid = taskid

    def get(self, num=None):
        pass

    def put(self, item):
        pass

    def task_done(self):
        """Indicate that a formerly enqueued task is complete. 

        Used by queue consumer process. For each get() used to fetch a task, 
        a subsequent call to task_done() tells the queue that the processing 
        on the task is complete and release the lock to next consumer.

        """

        pass


class _TaskingDoingLock(object):
    def __init__(self, lockdb, lockid):
        pass

    def acquire(self):
        pass

    def release(self):
        pass


class _TaskAppendingLock(object):

    def __init__(self, lockdb, lockid):
        pass

    def acquire(self):
        pass

    def release(self):
        pass
