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
"""
This module defines classes and global functions for creating and managing
connection to the graphlab backend server.
"""

""" The module usage metric tracking object """
from graphlab_util.config import DEFAULT_CONFIG as _default_local_conf
from graphlab_util.metric_tracker import MetricTracker as _MetricTracker


""" The global client object """
__CLIENT__ = None

""" The global graphlab server object """
__SERVER__ = None

""" The module usage metric tracking object """
__USAGE_METRICS__ = _MetricTracker(mode=_default_local_conf.mode)

__UNITY_GLOBAL_PROXY__ = None

def _get_metric_tracker():
    """
    Returns the global metric tracker object.
    """
    return __USAGE_METRICS__
