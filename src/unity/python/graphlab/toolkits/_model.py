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
"""@package graphlab.toolkits

Defines a basic interface for a model object.
"""
import graphlab.connect as _mt
import graphlab as _gl
from graphlab.toolkits._internal_utils import _map_unity_proxy_to_object
from graphlab.data_structures.sframe import SFrame as _SFrame
from graphlab.util import make_internal_url
import graphlab.connect.main as glconnect
import json


def load_model(location):
    """
    Load any GraphLab Create model that was previously saved.

    This function assumes the model (can be any model) was previously saved in
    GraphLab Create model format with model.save(filename).

    Parameters
    ----------
    location : string
        Location of the model to load. Can be a local path or a remote URL.
        Because models are saved as directories, there is no file extension.

    Examples
    ----------
    >>> model.save('my_model_file')
    >>> loaded_model = gl.load_model('my_model_file')
    """
    _mt._get_metric_tracker().track('toolkit.model.load_model')

    return glconnect.get_unity().load_model(make_internal_url(location))

def _get_default_options_wrapper(unity_server_model_name,
                                module_name='',
                                python_class_name='', 
                                sdk_model = False):
    """
    Internal function to return a get_default_options function.

    Parameters
    ----------
    unity_server_model_name: str
        Name of the class/toolkit as registered with the unity server

    module_name: str, optional
        Name of the module.

    python_class_name: str, optional
        Name of the Python class.

    sdk_model : bool, optional (default False)
        True if the SDK interface was used for the model. False otherwise.

    Examples
    ----------
    get_default_options = _get_default_options_wrapper('classifier_svm',
                                                       'svm', 'SVMClassifier')
    """
    def get_default_options_for_model(output_type = 'sframe'):
        """
        Get the default options for the toolkit 
        :class:`~graphlab.{module_name}.{python_class_name}`.

        Parameters
        ----------
        output_type : str, optional

            The output can be of the following types.

            - `sframe`: A table description each option used in the model.
            - `json`: A list of option dictionaries.

            | Each dictionary/row in the JSON/SFrame object describes the
              following parameters of the given model.

            +------------------+-------------------------------------------------------+
            |      Name        |                  Description                          |
            +==================+=======================================================+
            | name             | Name of the option used in the model.                 |
            +------------------+---------+---------------------------------------------+
            | description      | A detailed description of the option used.            |
            +------------------+-------------------------------------------------------+
            | type             | Option type (REAL, BOOL, INTEGER or CATEGORICAL)      |
            +------------------+-------------------------------------------------------+
            | default_value    | The default value for the option.                     |
            +------------------+-------------------------------------------------------+
            | possible_values  | List of acceptable values (CATEGORICAL only)          |
            +------------------+-------------------------------------------------------+
            | lower_bound      | Smallest acceptable value for this option (REAL only) |
            +------------------+-------------------------------------------------------+
            | upper_bound      | Largest acceptable value for this option (REAL only)  |
            +------------------+-------------------------------------------------------+

        Returns
        -------
        out : JSON/SFrame

        See Also
        --------
        graphlab.{module_name}.{python_class_name}.get_current_options

        Examples
        --------
        .. sourcecode:: python

          >>> import graphlab

          # Returns an output as an SFrame
          >>> out_sframe = graphlab.{module_name}.get_default_options()

          # Returns the output as a JSON
          >>> out_sframe = graphlab.{module_name}.get_default_options('json')
        """
        _mt._get_metric_tracker().track('toolkit.%s.get_default_options' % module_name)
        if sdk_model:
            response = _gl.extensions._toolkits_sdk_get_default_options(
                                                          unity_server_model_name)
        else:
            response = _gl.extensions._toolkits_get_default_options(
                                                          unity_server_model_name)
        
        for k in response.keys():
            response[k] = json.loads(response[k],
               parse_int = lambda x: float(x) if type(int(x)) is long else int(x))

        if output_type == 'json':
          return response
        else:
          json_list = [{'name': k, '': v} for k,v in response.items()]
          return _SFrame(json_list).unpack('X1', column_name_prefix='')\
                                   .unpack('X1', column_name_prefix='')

    # Change the doc string before returning.
    get_default_options_for_model.__doc__ = get_default_options_for_model.\
            __doc__.format(python_class_name = python_class_name,
                  module_name = module_name)
    return get_default_options_for_model


class Model(object):
    """
    This class defines the minimal interface of a model object
    storing the results from a toolkit function.

    User can query the model object via the `get` function, and
    the list of the queriable fields can be obtained via `list_fields`.

    Model object also supports dictionary type access, e.g. m['pagerank']
    is equivalent to m.get('pagerank').
    """

    """The UnityModel Proxy Object"""
    __proxy__ = None

    def name(self):
        """
        Returns the name of the model.

        Returns
        -------
        out : str
            The name of the model object.

        Examples
        --------
        >>> model_name = m.name()
        """
        return self.__class__.__name__

    def list_fields(self):
        """
        List of fields stored in the model. Each of these fields can be queried
        using the ``get(field)`` function or ``m[field]``.

        Returns
        -------
        out : list[str]
            A list of fields that can be queried using the ``get`` method.

        Examples
        --------
        >>> fields = m.list_fields()
        """
        _mt._get_metric_tracker().track(self.__class__.__module__ + '.list_fields')
        return self.__proxy__.list_fields()

    def get(self, field):
        """Return the value for the queried field.

        Each of these fields can be queried in one of two ways:

        >>> out = m['field']
        >>> out = m.get('field')  # equivalent to previous line

        Parameters
        ----------
        field : string
            Name of the field to be retrieved.

        Returns
        -------
        out : value
            The current value of the requested field.

        """
        if field in self.list_fields():
            return self.__proxy__.get(field)
        else:
            raise KeyError('Field \"%s\" not in model. Available fields are %s.' % (field, ', '.join(self.list_fields())))

    def summary(self):
        """
        Print a summary of the model.
        The summary includes a description of training
        data, options, hyper-parameters, and statistics measured during model
        creation.

        Examples
        --------
        >>> m.summary()
        """
        _mt._get_metric_tracker().track(self.__class__.__module__ + '.summary')
        print self.__repr__()

    def __getitem__(self, key):
        return self.get(key)

    def _get_wrapper(self):
        """Return a lambda function: UnityModel -> M, for constructing model
        class M from a UnityModel proxy."""
        raise NotImplementedError

    def save(self, location):
        """
        Save the model. The model is saved as a directory which can then be
        loaded using the :py:func:`~graphlab.load_model` method.

        Parameters
        ----------
        location : string
            Target destination for the model. Can be a local path or remote URL.

        See Also
        ----------
        graphlab.load_model

        Examples
        ----------
        >>> model.save('my_model_file')
        >>> loaded_model = gl.load_model('my_model_file')

        """
        _mt._get_metric_tracker().track('toolkit.model.save')
        return glconnect.get_unity().save_model(self, make_internal_url(location))

    def __repr__(self):
        raise NotImplementedError

    def __str__(self):
        return self.__repr__()


