# -*- coding: utf-8 -*-
"""
Simple taskqueue


Class:

TaskQueue


Function:

do_task

"""

from .queue import TaskQueue


def do_task(taskpool, taskid, workfunc, tasklock=True, tasktracing=True):
    """An all-in-one way to finish the task using TaskQueue system

    If empty queue or no items getten, return None, If cannot acquire the tasklook, 
    return False. Otherwise return the amount of the item.

    Args:

    taskpool - an sqlite3 database path to store the task information

    taskid - taskid in taskqueue table

    workfunc - a callback function to finsh the task one item one time, 
               If tasktracing is True, this function return True indicates 
               the last item is done ok, else fail.

    tasklock - (key-word), if True, Tasklock object will construct

    tasktracing - (key-word), if True, TaskTracing object will construct

    """

    q = TaskQueue(taskpool, taskid)
    if q.empty():
        return None
    if tasklock:
        tlock = q.tasklock()
        if not tlock.acquire():
            return False
    items = q.get()
    if not items:
        return None
    count = len(items)
    # no tracing
    if tasktracing:
        tracing = q.tasktracing(items)
        for i in range(count):
            r = workfunc(items[i])
            if r:
                tracing.ok(i)
            else:
                tracing.fail(i)
    else:
        for item in items:
            workfunc(item)
    if tasklock:
        tlock.release()
    return count
