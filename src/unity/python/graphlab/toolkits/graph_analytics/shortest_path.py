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
import copy as _copy

_HAS_IPYTHON = True
try:
    import IPython.core.display as _IPython
except:
    _HAS_IPYTHON = False


def get_default_options():
    """
    Get the default options for :func:`graphlab.shortest_path.create`.

    Returns
    -------
    out : dict

    See Also
    --------
    ShortestPathModel.get_current_options
    
    Examples
    --------
    >>> graphlab.shortest_path.get_default_options()
    """
    _mt._get_metric_tracker().track('toolkit.graph_analytics.sssp.get_default_options')

    return _main.run('sssp_default_options', {})


class ShortestPathModel(_ModelBase):
    """
    Model object containing the distance for each vertex in the graph to a
    single source vertex, which is specified during
    :func:`graphlab.shortest_path.create`.

    The model also allows querying for one of the shortest paths from the source
    vertex to any other vertex in the graph.

    Below is a list of queryable fields for this model:

    +----------------+------------------------------------------------------------+
    | Field          | Description                                                |
    +================+============================================================+
    | graph          | A new SGraph with the distance as a vertex property        |
    +----------------+------------------------------------------------------------+
    | distance       | An SFrame with each vertex's distance to the source vertex |
    +----------------+------------------------------------------------------------+
    | weight_field   | The edge field for weight                                  |
    +----------------+------------------------------------------------------------+
    | source_vid     | The source vertex id                                       |
    +----------------+------------------------------------------------------------+
    | max_distance   | Maximum distance between any two vertices                  |
    +----------------+------------------------------------------------------------+
    | training_time  | Total training time of the model                           |
    +----------------+------------------------------------------------------------+

    This model cannot be constructed directly.  Instead, use 
    :func:`graphlab.shortest_path.create` to create an instance
    of this model. A detailed list of parameter options and code samples 
    are available in the documentation for the create function.

    See Also
    --------
    create
    """
    def __init__(self, model):
        '''__init__(self)'''
        self.__proxy__ = model
        self._path_query_table = None

    def _result_fields(self):
        ret = super(ShortestPathModel, self)._result_fields()
        ret['distance'] = "SFrame with each vertex's distance. See m['distance']"
        return ret

    def _setting_fields(self):
        ret = super(ShortestPathModel, self)._setting_fields()
        for k in ['source_vid', 'weight_field', 'max_distance']:
            ret[k] = self[k]
        return ret

    def _method_fields(self):
        return {'get_path': 'Get shortest path, e.g. m.get_path(vid=target_vid)'}

    def get_path(self, vid, show=False, highlight=None, **kwargs):
        """
        Get the shortest path.
        Return one of the shortest paths between the source vertex defined
        in the model and the query vertex.
        The source vertex is specified by the original call to shortest path.
        Optionally, plots the path with networkx.

        Parameters
        ----------
        vid : string
            ID of the destination vertex. The source vertex ID is specified
            when the shortest path result is first computed.

        show : boolean
            Indicates whether the path should be plotted. Default is False.

        highlight : list
            If the path is plotted, identifies the vertices (by vertex ID) that
            should be highlighted by plotting in a different color.

        kwargs :
            Additional parameters passed into the :func:`graphlab.SGraph.show`
            when `show` is True.

        Returns
        -------
        path : list
            List of pairs of (vertex_id, distance) in the path.

        Examples
        --------
        >>> m.get_path(vid=0, show=True)

        See Also
        --------
        SGraph.show
        """
        if self._path_query_table is None:
            self._path_query_table = self._generate_path_sframe()

        source_vid = self['source_vid']
        path = []
        path_query_table = self._path_query_table
        if not vid in path_query_table['vid']:
            raise ValueError('Destination vertex id ' + str(vid) + ' not found')

        record = path_query_table[path_query_table['vid'] == vid][0]
        dist = record['distance']
        if dist > 1e5:
            raise ValueError('The distance to {} is too large to show the path.'.format(vid))

        path = [(vid, dist)]
        max_iter = len(path_query_table)
        num_iter = 0
        while record['distance'] != 0 and num_iter < max_iter:
            parent_id = record['parent_row_id']
            assert parent_id < len(path_query_table)
            assert parent_id >= 0
            record = path_query_table[parent_id]
            path.append((record['vid'], record['distance']))
            num_iter += 1
        assert record['vid'] == source_vid
        assert num_iter < max_iter
        path.reverse()

        if show is True and len(path) > 1:
            sub_g = _SGraph()
            for i, j in zip(path, path[1:]):
                sub_g = sub_g.add_edges(self['graph'].get_edges(src_ids=i, dst_ids=j),
                                        src_field='__src_id',
                                        dst_field='__dst_id')

            path_highlight = []

            if highlight is not None:

                if not isinstance(highlight, list):
                    raise TypeError("Input 'highlight' must be a list.")

                path_names = set([x[0] for x in path])
                path_highlight = list(set.intersection(path_names, set(highlight)))

            plot = sub_g.show(vlabel='id', highlight=path_highlight, **kwargs)
            if _HAS_IPYTHON:   
                _IPython.display(plot)
      
        return path

    def _generate_path_sframe(self):
        """
        Generates an sframe with columns: vid, parent_row_id, and distance.
        Used for speed up the path query.
        """
        source_vid = self['source_vid']
        weight_field = self['weight_field']

        query_table = _copy.copy(self.get('distance'))
        query_table = query_table.add_row_number('row_id')

        g = self.get('graph').add_vertices(query_table)
        # The sequence id which a vertex is visited, initialized with 0 meaning not visited.
        g.vertices['__parent__'] = -1
        weight_field = self['weight_field']
        if (weight_field == ""):
            weight_field = '__unit_weight__'
            g.edges[weight_field] = 1

        # Traverse the graph once and get the parent row id for each vertex
        # def traverse_fun(src, edge, dst):
        #     if src['__id'] == source_vid:
        #         src['__parent__'] = src['row_id']
        #     if dst['distance'] == src['distance'] + edge[weight_field]:
        #         dst['__parent__'] = max(dst['__parent__'], src['row_id'])
        #     return (src, edge, dst)
        #
        # the internal lambda appear to have some issues. 
        import graphlab
        traverse_fun = lambda src, edge, dst:  \
            graphlab.extensions._toolkits.graph.sssp.shortest_path_traverse_function(src, edge, dst,
                    source_vid, weight_field)

        g = g.triple_apply(traverse_fun, ['__parent__'])
        query_table = query_table.join(g.get_vertices()[['__id', '__parent__']], '__id').sort('row_id')
        query_table.rename({'__parent__': 'parent_row_id', '__id': 'vid'})
        return query_table


def create(graph, source_vid, weight_field="", max_distance=1e30, verbose=True):
    """
    Compute the single source shortest path distance from the source vertex to
    all vertices in the graph. Note that because SGraph is directed, shortest
    paths are also directed. To find undirected shortes paths add edges to the
    SGraph in both directions. Return a model object with distance each of
    vertex in the graph.

    Parameters
    ----------
    graph : SGraph
        The graph on which to compute shortest paths.

    source_vid : vertex ID
        ID of the source vertex.

    weight_field : string, optional
        The edge field representing the edge weights. If empty, uses unit
        weights.

    verbose : bool, optional
        If True, print progress updates.

    Returns
    -------
    out : ShortestPathModel

    References
    ----------
    - `Wikipedia - ShortestPath <http://en.wikipedia.org/wiki/Shortest_path_problem>`_

    Examples
    --------
    If given an :class:`~graphlab.SGraph` ``g``, we can create
    a :class:`~graphlab.shortest_path.ShortestPathModel` as follows:

    >>> g = graphlab.load_graph('http://snap.stanford.edu/data/email-Enron.txt.gz', format='snap')
    >>> sp = graphlab.shortest_path.create(g, source_vid=1)

    We can obtain the shortest path distance from the source vertex to each
    vertex in the graph ``g`` as follows:

    >>> sp_sframe = sp['distance']   # SFrame

    To get the actual path from the source vertex to any destination vertex:

    >>> path = sp.get_path(vid=10)


    We can obtain an auxiliary graph with additional information corresponding
    to the shortest path from the source vertex to each vertex in the graph
    ``g`` as follows:

    >>> sp_graph = sp.get['graph']       # SGraph

    See Also
    --------
    ShortestPathModel
    """
    _mt._get_metric_tracker().track('toolkit.graph_analytics.sssp.create')

    if not isinstance(graph, _SGraph):
        raise TypeError('graph input must be a SGraph object.')

    opts = {'source_vid': source_vid, 'weight_field': weight_field,
            'max_distance': max_distance, 'graph': graph.__proxy__}
    params = _main.run('sssp', opts, verbose)
    return ShortestPathModel(params['model'])


def _compute_shortest_path(graph, source_vids, dest_vids, weight_field=""):
    """
    Computes shortest paths from any vertex in source_vids to any vertex
    in dest_vids.  Note that because SGraph is directed, shortest paths are
    also directed. To find undirected shortes paths add edges to the SGraph in
    both directions. Returns a list of shortest paths between source_vids 
    and dest_vids.

    Note that this function does not compute all shortest paths between every 
    (source, dest) pair. It computes 

    Parameters
    ----------
    graph : SGraph
        The graph on which to compute shortest paths.

    source_vids : vertex ID or list of vertex IDs
        ID of the source vertices

    dest_vids : vertex ID or list of vertex IDs
        ID of the destination vertices

    weight_field : str, optional.
        The edge field representing the edge weights. If empty, uses unit
        weights.

    Returns
    -------
    out :  An SArray of lists of all the same length. 
        Each list describes a path of vertices leading from one source
        vertex to one destination vertex.

    References
    ----------
    - `Wikipedia - ShortestPath <http://en.wikipedia.org/wiki/Shortest_path_problem>`_

    Examples
    --------
    If given an :class:`~graphlab.SGraph` ``g``, we can create
    a :class:`~graphlab.shortest_path.ShortestPathModel` as follows:

    >>> edge_src_ids = ['src1', 'src2',   'a', 'b', 'c'  ]
    >>> edge_dst_ids = [   'a',    'b', 'dst', 'c', 'dst']
    >>> edges = graphlab.SFrame({'__src_id': edge_src_ids, '__dst_id': edge_dst_ids})
    >>> g=gl.SGraph().add_edges(edges)
    >>> graphlab.shortest_path.compute_shortest_path(g, ["src1","src2"], "dst")
    [['a','dst']]

    See Also
    --------
    ShortestPathModel
    """
    _mt._get_metric_tracker().track('toolkit.graph_analytics.shortest_path.get_shortest_path')
    if type(source_vids) != list:
        source_vids = [source_vids]
    if type(dest_vids) != list:
        dest_vids = [dest_vids]
    import graphlab
    return graphlab.SArray(graphlab.extensions._toolkits.graph.sssp.all_shortest_paths(graph, source_vids, dest_vids, weight_field))
