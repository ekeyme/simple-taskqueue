# -*- coding: utf-8 -*-
"""helper functions"""

import sqlite3

def setup_taskqueue_tables(taskpool):
    """Create taskqueue tables in taskpool sqlite3 file"""

    import json
    from os.path import abspath, dirname, join

    db = sqlite3.connect(taskpool)
    with open(join(dirname(abspath(__file__)), 'setup.json'), 
              encoding='utf-8') as configf:
        config = json.load(configf)
    for s in config['setup_sql']:
        db.execute(s)
    db.commit()
    db.close
    return True
    