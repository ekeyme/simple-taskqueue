# -*- coding: utf-8 -*-
"""
Queue module

This module implement a simeple queue data structrue basing on sqlite3.


Class:

TaskQueue

"""

import sqlite3
import json
import time
from .tracing import TaskTracing
from .tasklock import TaskLock


# Max try-time of queuelock to acquire the lock
QUEUE_LOCK_TRY_TIME = 10


class TaskQueue(object):
    def __init__(self, taskpool, taskid):
        """
        Args:

        taskpool - an sqlite3 database path to store the task information
        
        taskid - taskid in taskqueue table

        Method:

        def get(self, num=None)
            Remove items from queue and return them

        def put(self, items)
            Put items into the queue

        def empty(self)
            Check if items in the queue

        def tasktracing(self, items)
            Construct a taskqueue.tracing.TaskTracing object, and return it

        def tasklock(self)
            Construct a taskqueue.tracing.TaskLock object, and return it

        """

        self.db = sqlite3.connect(taskpool)
        self._taskinfo = get_taskinfo(self.db, taskid)
        if self._taskinfo is None:
            raise KeyError('unrecognized taskid: {}'.format(taskid))
        self.taskid = self._taskinfo['taskid']
        self.lockid = self._taskinfo['lockid']
        self._queuelock = _QueueLock(self.db, self.taskid)
        self._tasklock = None

    def get(self, num=None):
        """Get num of items from the queue
        
        If num is None or num > len(items), then all of items will be return, 
        else num of items will be return. num must be > 0, otherwise raise
        KeyError.

        """

        if num is not None and num < 0:
            raise KeyError("num must be larger than 0")

        self._queuelock.acquire()
        items = self._get()
        if num is None:
            num = len(items)
        r, items_left = items[:num], items[num:]
        self._put(items_left)
        self._queuelock.release()
        return items

    def put(self, items):
        """Put items into the queue
        
        Args:

        items - items must be iterable, we use json.loads(items) to serialize it

        """

        self._queuelock.acquire()
        item_list = self._get()
        item_list.extend(items)
        self._put(item_list)
        self._queuelock.release()

    def empty(self):
        """Check if the queue item is empty"""

        return not bool(self._get())

    def tasktracing(self, items):
        """Return tasktracing object for tracing"""
        
        return TaskTracing(self.db, self.taskid, items)

    def tasklock(self):
        """Return tasklock object, if no lock raise ValueError"""

        if self._tasklock is None:
            self._tasklock = TaskLock(self.db, self.taskid, self.lockid)
        return self._tasklock

    def _put(self, items):
        """Put items into queue without checking tasklock and without concatenation"""

        s = json.dumps(items)
        self.db.execute("""UPDATE taskqueue SET items=?, 
                            update_time=datetime('now', 'localtime') 
                           WHERE taskid=?""", (s, self.taskid))
        self.db.commit()

    def _get(self):
        """Get all items without checking tasklock"""

        s = self.db.execute("""SELECT items FROM taskqueue 
                               WHERE taskid=? LIMIT 1""", 
                            (self.taskid, )).fetchone()[0]
        if s == '':
            s = '[]'
        return json.loads(s)

    def __del__(self):
        self.db.close()


class _QueueLock(object):
    """Lock handler object for taskqueue
    
    Used to separate the mutiple get and put action on the queue.

    Args:

    lockdb - taskpool database connection

    taskid - taskid in taskqueue table
    
    """

    def __init__(self, lockdb, taskid):
        self.db = lockdb
        self.taskid = taskid

    def acquire(self):
        """acquire the lock
        
        If the qlocked is not locked by other process, then return True,
        else first try taskqueue.queue.QUEUE_LOCK_TRY_TIME times 
        untill get the lock, finally if still cannot get the lock raise 
        taskqueue.queue.QueueLockTimeOut.

        """

        i = QUEUE_LOCK_TRY_TIME
        while i > 0:
            if not self.locked():
                self.lock()
                return True
            time.sleep(1)
            i -= 1
        raise QueueLockTimeOut("failed after 10 times trying.")

    def release(self):
        """Release qlocked"""

        self.db.execute("""UPDATE taskqueue SET qlocked=?, 
                            update_time=datetime('now', 'localtime') 
                           WHERE taskid=?""", (0, self.taskid))
        self.db.commit()

    def locked(self):
        """Check if locked"""

        qlocked = self.db.execute("""SELECT qlocked FROM taskqueue 
                                     WHERE taskid=? LIMIT 1""", 
                                     (self.taskid, )).fetchone()[0]
        assert qlocked in (0, 1), "invalid qlocked type: {}".format(qlocked)
        if qlocked == 1:
            return True
        return False

    def lock(self):
        """make locked"""

        self.db.execute("""UPDATE taskqueue SET qlocked=?, 
                            update_time=datetime('now', 'localtime') 
                           WHERE taskid=?""", (1, self.taskid))
        self.db.commit()


class QueueLockTimeOut(RuntimeError):
    """Raise when queuelock is still locked after trying for given seconds"""

    pass


def get_taskinfo(db, taskid):
    """Get task information"""

    ori_row_factory = db.row_factory
    db.row_factory = sqlite3.Row
    r = db.execute("""SELECT taskid, que.lockid, que.items, que.qlocked, 
                        que.desc AS queue_desc, locked, current_taskid, 
                        lck.desc AS lock_desc 
                      FROM taskqueue AS que LEFT JOIN tasklock AS lck 
                      ON que.lockid = lck.lockid WHERE taskid = ? LIMIT 1""", 
                  (taskid, )).fetchone()
    db.row_factory = ori_row_factory
    return r
