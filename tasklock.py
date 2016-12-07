# -*- coding: utf-8 -*-
"""
Task lock

This module implement a primitive lock for some tasks which need to separate
other processes to avoid getting items from the queue to do the jobs which 
need to avoid colliding, e.g. update some row in the database table.

"""

import sqlite3

class TaskLock(object):
    """Lock object for doing task
    
    Args:

    lockdb - taskpool database connection

    taskid - taskid in taskqueue table

    lockid - lockid in tasklock table


    Method:

    def acquire(self)
        If acquire lock successfully, then return True, eles False.

    def release(self)
        Release the lock

    """

    def __init__(self, lockdb, taskid, lockid):
        self.db = lockdb
        self.taskid = taskid
        self.lockid = lockid
        if get_tasklockinfo(self.db, lockid) is None:
            raise KeyError("no such lock: {}".format(lockid))
        self.locked_by_self = None

    def acquire(self):
        """Acquire a lock
        
        If success return True, else False.

        """

        if self.locked():
            self.locked_by_self = False
            return False
        else:
            self.lock()
            self.locked_by_self = True
            return True

    def release(self):
        """Release the lock

        If this lock is not owned by your this process or not locked, 
        raise RuntimeError.

        """

        if not self.locked_by_self:
            raise RuntimeError("can not release the lock which is not owned by this process")
        if not self.locked():
            raise RuntimeError("can not release the lock which is not locked")
        self.db.execute("""UPDATE tasklock SET locked=?, 
                            update_time=datetime('now', 'localtime') 
                           WHERE lockid=?""", (0, self.lockid))
        self.db.commit()

    def locked(self):
        """check if locked"""

        locked = get_tasklockinfo(self.db, self.lockid)['locked']
        assert locked in (0, 1), "invalid locked type: {}".format(locked)
        return bool(locked)

    def lock(self):
        """make a lock"""

        self.db.execute("""UPDATE tasklock SET locked=?, current_taskid=?, 
                            update_time=datetime('now', 'localtime') 
                           WHERE lockid=?""", (1, self.taskid, self.lockid))
        self.db.commit()


def get_tasklockinfo(db, lockid):
    """Get task lock information"""

    ori_row_factory = db.row_factory
    db.row_factory = sqlite3.Row
    r = db.execute("""SELECT lockid, locked, current_taskid, desc, update_time 
                      FROM tasklock WHERE lockid = ? LIMIT 1""", 
                   (lockid, )).fetchone()
    db.row_factory = ori_row_factory
    return r
