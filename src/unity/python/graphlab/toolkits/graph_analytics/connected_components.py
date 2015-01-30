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
import graphlab.connect as _mt
from graphlab.data_structures.sgraph import SGraph as _SGraph
import graphlab.toolkits._main as _main
from graphlab.toolkits.graph_analytics._model_base import GraphAnalyticsModel as _ModelBase


def get_default_options():
    """
    Get the default options for :func:`graphlab.connected_components.create`.

    Returns
    -------
    out : dict

    Examples
    --------
    >>> graphlab.connected_components.get_default_options()
    """
    _mt._get_metric_tracker().track('toolkit.graph_analytics.connected_components.get_default_options')
    return _main.run('connected_components_default_options', {})


class ConnectedComponentsModel(_ModelBase):
    r"""
    A ConnectedComponentsModel object contains the component ID for each vertex 
    and the total number of weakly connected components in the graph.

    A weakly connected component is a maximal set of vertices such that there
    exists an undirected path between any two vertices in the set.

    Below is a list of queryable fields for this model:

    +----------------+-----------------------------------------------------+
    | Field          | Description                                         |
    +================+=====================================================+
    | graph          | A new SGraph with the color id as a vertex property |
    +----------------+-----------------------------------------------------+
    | training_time  | Total training time of the model                    |
    +----------------+-----------------------------------------------------+
    | component_size | An SFrame with the size of each component           |
    +----------------+-----------------------------------------------------+
    | component_id   | An SFrame with each vertex's component id           |
    +----------------+-----------------------------------------------------+

    This model cannot be constructed directly.  Instead, use 
    :func:`graphlab.connected_components.create` to create an instance
    of this model. A detailed list of parameter options and code samples 
    are available in the documentation for the create function.

    See Also
    --------
    create
    """
    def __init__(self, model):
        '''__init__(self)'''
        self.__proxy__ = model
        self.__model_name__ = "connected_components"

    def _result_fields(self):
        ret = super(ConnectedComponentsModel, self)._result_fields()
        ret['component_size'] = "SFrame with each component's size. See m['component_size']"
        ret['component_id'] = "SFrame with each vertex's component id. See m['componentid']"
        return ret


def create(graph, verbose=True):
    """
    Compute the number of weakly connected components in the graph. Return a
    model object with total number of weakly connected components as well as the
    component ID for each vertex in the graph.

    Parameters
    ----------
    graph : SGraph
        The graph on which to compute the triangle counts.

    verbose : bool, optional
        If True, print progress updates.

    Returns
    -------
    out : ConnectedComponentsModel

    References
    ----------
    - `Mathworld Wolfram - Weakly Connected Component
      <http://mathworld.wolfram.com/WeaklyConnectedComponent.html>`_

    Examples
    --------
    If given an :class:`~graphlab.SGraph` ``g``, we can create
    a :class:`~graphlab.connected_components.ConnectedComponentsModel` as
    follows:

    >>> g = graphlab.load_graph('http://snap.stanford.edu/data/email-Enron.txt.gz', format='snap')
    >>> cc = graphlab.connected_components.create(g)
    >>> cc.summary()

    We can obtain the ``component id`` corresponding to each vertex in the
    graph ``g`` as follows:

    >>> cc_ids = cc['component_id']  # SFrame

    We can obtain a graph with additional information about the ``component
    id`` corresponding to each vertex as follows:

    >>> cc_graph = cc['graph']      # SGraph

    See Also
    --------
    ConnectedComponentsModel
    """
    _mt._get_metric_tracker().track('toolkit.graph_analytics.connected_components.create')

    if not isinstance(graph, _SGraph):
        raise TypeError('graph input must be a SGraph object.')

    params = _main.run('connected_components', {'graph': graph.__proxy__},
                       verbose)
    return ConnectedComponentsModel(params['model'])
