# -*- coding: utf-8 -*-
"""
Simple taskqueue

"""

import json
import sqlite3
import time


class TaskQueue(object):
    def __init__(self, taskpool, taskid):
        """
        Args:
            taskpool - an sqlite3 database path to store the task information
            taskid - task id

        """

        self.db = sqlite3.connect(taskpool)
        self._taskinfo = get_taskinfo(self.db, taskid)
        self.taskid = self._taskinfo['taskid']
        self.lockid = self._taskinfo['lockid']
        self._queuelock = _QueueLock(self.db, self.taskid)
        self._tasklock = None

    def get(self, num=None):
        """Get num of items from the queue
        
        If num is None or num > len(items), then all of items will be return, 
        else num of items will be return.

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

    def empty(self):
        """Check if the queue item is empty"""

        return bool(self._get())

    def tasktracing(self, items):
        """Return tasktracing object for tracing"""
        
        return TaskTracing(self.db, self.taskid, items)

    def tasklock(self):
        """Return tasklock object, if no lock raise ValueError"""
        
        if self.lockid is None:
            raise ValueError("No lock for task: {}".format(self.taskid))
        if self._tasklock is None:
            self._tasklock = TaskLock(self.db, self.taskid, self.lockid)
        return self._tasklock

    def __del__(self):
        self.db.close()


class _QueueLock(object):
    """Lock handler object for taskqueue

    Args:

    lockdb - taskpool database connector
    taskid - 
    
    """

    def __init__(self, lockdb, taskid):
        self.db = lockdb
        self.timeout = 10
        self.taskid = taskid

    def acquire(self):
        """acquire the lock
        
        If the qlocked is not locked by other process, then return True,
        else first try 10 times(in about 10 seconds) untill get the lock, 
        finally if still cannot get the lock raise taskqueue.QueueLockTimeOut.

        """

        i = self.timeout
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


class TaskLock(object):
    """Lock object for doing task
    
    Args:

    lockdb - taskpool database connector
    taskid - 
    lockid - 

    """

    def __init__(self, lockdb, taskid, lockid):
        self.db = lockdb
        self.taskid = taskid
        self.lockid = lockid

    def acquire(self):
        """Acquire a lock
        
        If success return True, else False.

        """

        if self.locked():
            return False
        else:
            self.lock()
            return True

    def release(self):
        """release a lock"""

        self.db.execute("""UPDATE tasklock SET locked=?, 
                            update_time=datetime('now', 'localtime') 
                           WHERE lockid=?""", (0, self.lockid))
        self.db.commit()

    def locked(self):
        """check locked"""

        locked = self.db.execute("""SELECT locked FROM tasklock 
                                    WHERE lockid=? LIMIT 1""", 
                                 (self.lockid, )).fetchone()[0]
        assert locked in (0, 1), "invalid locked type: {}".format(locked)
        return bool(locked)

    def lock(self):
        """make a lock"""

        self.db.execute("""UPDATE tasklock SET locked=?, current_taskid=?, 
                            update_time=datetime('now', 'localtime') 
                           WHERE lockid=?""", (1, self.taskid, self.lockid))
        self.db.commit()


class TaskTracing(object):
    """For tracing task doing
    
    Args:

    tracedb - taskpool database connection
    taskid - 
    items - task items to be finish

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
        """Indicates the currenct finished item is done ok"""

        self._tracing('{}:ok'.format(markname))

    def fail(self, markname):
        """Indicates the currenct finished item is done fail"""

        self._tracing('{}:fail'.format(markname))

    def _tracing(self, s):
        """Upate tracing table"""

        self.tracing_items.append(s)
        self.cur.execute("""UPDATE tasktracing SET tracing=? WHERE id=?""", 
                         (','.join(self.tracing_items), self.id))
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


def do_task(taskqueue, workfunc, tasklock=True, tracing=True):
    pass
