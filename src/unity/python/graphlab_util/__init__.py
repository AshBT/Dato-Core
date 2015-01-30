'''
Copyright (C) 2015 Dato, Inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''
# 1st set up logging
__all__ = ['psutil']
import logging
from logging.handlers import TimedRotatingFileHandler
import logging.config
import time
import tempfile
import os
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
        },
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
        },
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
