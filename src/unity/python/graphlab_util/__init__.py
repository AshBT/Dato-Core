'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''
# 1st set up logging
__all__ = ['psutil']
import logging
from logging.handlers import TimedRotatingFileHandler
import logging.config
import time
import tempfile
import os
from Queue import Queue as queue

from queue_channel import QueueHandler, MutableQueueListener

# overuse the same 'graphlab' logger so we have one logging config
logger = logging.getLogger('graphlab')
client_log_file = os.path.join(tempfile.gettempdir(), ('graphlab_client_%d.log' % time.time()))

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
        'brief': {
            'format': '[%(levelname)s] %(message)s'
        }
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'brief'
        },
        'file': {
            'level':'DEBUG',
            'class':'logging.handlers.TimedRotatingFileHandler',
            'formatter':'standard',
            'filename':client_log_file,
            'when':'H',
            'interval': 1,
            'backupCount': 5,
            'encoding': 'UTF-8',
            'delay': 'False',
            'utc': 'True'
        }
    },
    'loggers': {
        '': {
            'handlers': ['default', 'file'],
            'level': 'INFO',
            'propagate': 'True'
        }
    }
})

logging.getLogger(__name__).addHandler(logging.NullHandler())
logging.getLogger('librato').setLevel(logging.CRITICAL)

#amend the logging configuration with a handler streaming to a message queue

q = queue(-1)
ql = MutableQueueListener(q)

qh = QueueHandler(q)
logging.root.addHandler(qh)

ql.start()

def stop_queue_listener():
    ql.stop()

def _attach_log_handler(handler):
    ql.addHandler(handler)

def _detach_log_handler(handler):
    ql.removeHandler(handler)
