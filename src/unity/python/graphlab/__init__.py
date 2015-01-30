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
@package graphlab
...
GraphLab Create is a machine learning platform that enables data scientists and app developers to easily create intelligent apps at scale. Building an intelligent, predictive application involves iterating over multiple steps: cleaning the data, developing features, training a model, and creating and maintaining a predictive service. GraphLab Create does all of this in one platform. It is easy to use, fast, and powerful.

For more details on the GraphLab Create see http://dato.com, including
documentation, tutorial, etc.

"""

import graphlab.connect.aws as aws

from graphlab.util import _check_canvas_enabled

from graphlab.data_structures.sgraph import Vertex, Edge
from graphlab.data_structures.sgraph import SGraph
from graphlab.data_structures.sarray import SArray
from graphlab.data_structures.sframe import SFrame
from graphlab.data_structures.sketch import Sketch
from graphlab.data_structures.image import Image


from graphlab.toolkits._model import Model

import graphlab.aggregate
import graphlab.toolkits
import graphlab.toolkits.graph_analytics as graph_analytics

from graphlab.toolkits.graph_analytics import connected_components
from graphlab.toolkits.graph_analytics import shortest_path
from graphlab.toolkits.graph_analytics import kcore
from graphlab.toolkits.graph_analytics import pagerank
from graphlab.toolkits.graph_analytics import graph_coloring
from graphlab.toolkits.graph_analytics import triangle_counting

## bring load functions to the top level
from graphlab.data_structures.sgraph import load_sgraph, load_graph
from graphlab.data_structures.sframe import load_sframe, get_spark_integration_jar_path
from graphlab.toolkits._model import load_model
from graphlab.data_structures.DBConnection import connect_odbc, get_libodbc_path, set_libodbc_path

# internal util
from graphlab.connect.main import launch as _launch
from graphlab.connect.main import stop as _stop
import graphlab.connect.main as glconnect

# python egg version
__VERSION__ = '{{VERSION_STRING}}'
version = '{{VERSION_STRING}}'

from graphlab.util import get_newest_version
from graphlab.util import perform_version_check
from graphlab.util import get_environment_config
from graphlab.util import get_runtime_config
from graphlab.util import set_runtime_config
from graphlab.util import get_graphlab_object_type

from graphlab.version_info import version
from graphlab.version_info import __VERSION__


class DeprecationHelper(object):
    def __init__(self, new_target):
        self.new_target = new_target

    def _warn(self):
        import warnings
        import logging
        warnings.warn("Graph has been renamed to SGraph. The Graph class will be removed in the next release.", PendingDeprecationWarning)
        logging.warning("Graph has been renamed to SGraph. The Graph class will be removed in the next release.")

    def __call__(self, *args, **kwargs):
        self._warn()
        return self.new_target(*args, **kwargs)

    def __getattr__(self, attr):
        self._warn()
        return getattr(self.new_target, attr)

Graph = DeprecationHelper(SGraph)

perform_version_check()

################### Extension Importing ########################
import graphlab.extensions
from graphlab.extensions import ext_import

graphlab.extensions._add_meta_path()

# rewrite the extensions module
class _extensions_wrapper(object):
  def __init__(self, wrapped):
    self._wrapped = wrapped
    self.__doc__ = wrapped.__doc__

  def __getattr__(self, name):
    try:
        return getattr(self._wrapped, name)
    except:
        pass
    graphlab.connect.main.get_unity()
    return getattr(self._wrapped, name)

import sys as _sys
_sys.modules["graphlab.extensions"] = _extensions_wrapper(_sys.modules["graphlab.extensions"])
# rewrite the import
extensions = _sys.modules["graphlab.extensions"]
