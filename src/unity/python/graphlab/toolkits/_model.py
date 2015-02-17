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
import tempfile
import json

import graphlab as _gl
import graphlab.connect as _mt
import graphlab.connect.main as glconnect
from graphlab.data_structures.sframe import SFrame as _SFrame
from graphlab.toolkits._internal_utils import _map_unity_proxy_to_object
from graphlab.util import _make_internal_url
from graphlab.util import gl_pickle
from pickle import PicklingError

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

    return glconnect.get_unity().load_model(_make_internal_url(location))

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

          # SFrame formatted output.
          >>> out_sframe = graphlab.{module_name}.get_default_options()

          # JSON formatted output.
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


def _get_class_full_name(obj):
    """
    Returns a full class name from the module name and the class name.

    Parameters
    ----------
    cls : type | object

    Returns
    -------
    A full name with all the imports.
    """
    cls_name = obj.__name__ if type(obj) == type else obj.__class__.__name__
    return "%s.%s" % (obj.__module__ , cls_name)


def _get_class_from_name(class_name, *arg, **kwarg):
    """
    Create a class instance given the name of the class. This will ensure all
    the required modules are imported.

    For example. The class graphlab.my_model will first import graphlab
    and then load my_model.

    """
    module_path = class_name.split('.')
    import_path = module_path[0:-1]
    module = __import__('.'.join(import_path), fromlist=[module_path[-1]])
    class_ = getattr(module, module_path[-1])
    return class_

class CustomModel(object):
    """
    This class defines the minimal interface of a model that can be interfaced
    with GraphLab objects. This class contains serialization routines that make
    it compatible with GraphLab objects.

    Examples
    ----------
    # Define the model
    def class MyModel(CustomModel):
        def __init__(self, sf, classifier):
            self.sframe = sf
            self.classifier = classifier

        def my_func(self):
            return self.classifier.predict(self.sframe)

    # Construct the model
    >>> custom_model = MyModel(sf, glc_model)

    ## The model can be saved and loaded like any GraphLab model.
    >>> model.save('my_model_file')
    >>> loaded_model = gl.load_model('my_model_file')
    """

    __proxy__ = None
    _PYTHON_MODEL_VERSION = 0

    def __init__(self, proxy = None):
        """
        Construct a dummy object from its proxy object and class name. The
        proxy contains a location to the pickle-file where the real object 
        is stored.

        Parameters
        ----------
        proxy  : object (type gl.extensions class)

        _class : class
                Name of the child class.

        Returns
        -------
        Returns an object of type _class with a path to the pickle archive 
        saved in proxy.temp_file.
 
        """
        if not proxy:
            self.__proxy__ = _gl.extensions._PythonModel()
        else:
            self.__proxy__ = proxy


    @classmethod
    def _is_gl_pickle_safe(cls):
        """
        Return True if the model is GLPickle safe i.e if the model does not 
        contain elements that are written using Python + GraphLab objects.
        """
        return True

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
        # Save to a temoporary pickle file.
        temp_file = tempfile.mktemp()
        self._save_to_pickle(temp_file)

        # Write the pickle file to an OARC
        if not self.__proxy__:
            self.__proxy__ = _gl.extensions._PythonModel()

        # The proxy contains the file.
        self.__proxy__.temp_file = temp_file
        wrapper = self._get_wrapper()
        return glconnect.get_unity().save_model(self.__proxy__,
                          _make_internal_url(location), wrapper)

    def _save_to_pickle(self, filename):
        """
        Save the object to a pickle file.

        Parameters
        ----------
        filename : Filename to save.

        Notes
        -----
        The file is saved to a GLPickle archive. The following three attributes
        are saved:

        - The version of the object (obtained from get_version())
        - Class name of the object.
        - The object is pickled as directed in _save_impl

        """
        if not self._is_gl_pickle_safe():
            raise RuntimeError("Cannot pickle object. Use the 'save' method.")

        # Setup the pickler.
        pickler = gl_pickle.GLPickler(filename)

        # Save version and class-name.
        pickler.dump(self._get_version())
        pickler.dump(_get_class_full_name(self))

        # Save the object.
        self._save_impl(pickler)
        pickler.close()

    def _save_impl(self, pickler):
        """
        An function to implement save for the object in consideration.
        The default implementation will dump self to the pickler.

        WARNING: This implementation is very simple.
                 Overwrite for smarter implementations.

        Parameters
        ----------
        pickler : An opened GLPickle archive (Do not close the archive.)
        """
        if not self._is_gl_pickle_safe():
            raise RuntimeError("Cannot pickle object. Use the 'save' method.")

        # Pickle will hate the proxy
        self.__proxy__ = None
        pickler.dump(self)

    def _get_wrapper(self):
        """
        Return a function: UnityModel -> M, for constructing model
        class M from a UnityModel proxy.
        """
        _class = self.__class__
        proxy_wrapper = self.__proxy__._get_wrapper()

        # Define the function
        def model_wrapper(unity_proxy):

            # Load the proxy object. This returns a proxy object with 
            # 'temp_file' set to where the object is pickled.
            model_proxy = proxy_wrapper(unity_proxy)
            temp_file = model_proxy.temp_file

            # Setup the unpickler.
            unpickler = gl_pickle.GLUnpickler(temp_file)
            
            # Get the version 
            version = unpickler.load()
            
            # Load the class name.
            cls_name = unpickler.load()
            cls = _get_class_from_name(cls_name)
            
            # Load the object with the right version. 
            obj = cls._load_version(unpickler, version)
            
            # Return the object 
            return obj

        return model_wrapper

    def _get_version(self):
        return self._PYTHON_MODEL_VERSION

    @classmethod
    def _load_version(cls, unpickler, version):
        """
        An function to load an object with a specific version of the class.

        WARNING: This implementation is very simple.
                 Overwrite for smarter implementations.

        Parameters
        ----------
        unpickler : GLUnpickler
            A GLUnpickler file handle.

        version : int
            A version number as maintained by the class writer.
        """
        # Warning: This implementation is by default not version safe.
        # For version safe implementations, please write the logic 
        # that is suitable for your model.
        return unpickler.load()
        

class Model(CustomModel):
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
            raise KeyError('Field \"%s\" not in model. Available fields are'
                         '%s.') % (field, ', '.join(self.list_fields()))

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
        >>> loaded_model = graphlab.load_model('my_model_file')

        """
        _mt._get_metric_tracker().track('toolkit.model.save')
        return glconnect.get_unity().save_model(self, _make_internal_url(location))

    def __repr__(self):
        raise NotImplementedError

    def __str__(self):
        return self.__repr__()

    @classmethod
    def _is_gl_pickle_safe(cls):
        """
        Return True if the model is GLPickle safe i.e if the model does not 
        contain elements that are written using Python + GraphLab objects.
        """
        return False

