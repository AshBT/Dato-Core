'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''
import graphlab as _gl
import graphlab_util.cloudpickle as _cloudpickle

import inspect as _inspect
import pickle as _pickle
import uuid as _uuid
import os as _os
import tempfile as _tempfile
import zipfile as _zipfile
import glob as _glob


def _get_temp_filename():
    tmp_file = _tempfile.NamedTemporaryFile()
    tmp_file.close()
    return tmp_file.name

def _is_not_pickle_safe_gl_model_class(obj_class):
    """
    Check if a GraphLab create model is pickle safe.

    The function does it by checking that _CustomModel is the base class.  

    Parameters
    ----------
    obj_class    : Class to be checked. 

    Returns
    ----------
    True if the GLC class is a model and is pickle safe.

    """
    if issubclass(obj_class, _gl.toolkits._model.CustomModel): 
        return not obj_class._is_gl_pickle_safe()
    return False

def _is_not_pickle_safe_gl_class(obj_class):
    """
    Check if class is a GraphLab create model. 

    The function does it by checking the method resolution order (MRO) of the
    class and verifies that _Model is the base class.  

    Parameters
    ----------
    obj_class    : Class to be checked. 

    Returns
    ----------
    True if the class is a GLC Model.

    """
    gl_ds = [_gl.SFrame, _gl.SArray, _gl.SGraph]

    # Object is GLC-DS or GLC-Model
    return (obj_class in gl_ds) or _is_not_pickle_safe_gl_model_class(obj_class)

def _get_gl_class_type(obj_class):
    """
    Internal util to get the type of the GLC class. The pickle file stores
    this name so that it knows how to construct the object on unpickling.

    Parameters
    ----------
    obj_class    : Class which has to be categoriized.

    Returns
    ----------
    A class type for the pickle file to save.

    """

    if obj_class == _gl.SFrame:
        return "SFrame"
    elif obj_class == _gl.SGraph:
        return "SGraph"
    elif obj_class == _gl.SArray:
        return "SArray"
    elif _is_not_pickle_safe_gl_model_class(obj_class):
        return "Model"
    else:
        return None

def _get_gl_object_from_persistent_id(type_tag, gl_archive_abs_path):
    """
    Internal util to get a GLC object from a persistent ID in the pickle file.

    Parameters
    ----------
    type_tag : The name of the glc class as saved in the GLC pickler.

    gl_archive_abs_path: An absolute path to the GLC archive where the 
                          object was saved.

    Returns
    ----------
    The GLC object.

    """
    if type_tag == "SFrame":
        obj = _gl.SFrame(gl_archive_abs_path)
    elif type_tag == "SGraph":
        obj = _gl.load_graph(gl_archive_abs_path)
    elif type_tag == "SArray":
        obj = _gl.SArray(gl_archive_abs_path)
    elif type_tag == "Model":
        obj = _gl.load_model(gl_archive_abs_path)
    else:
        raise _pickle.UnpicklingError("GraphLab pickling Error: Unspported object."
              " Only SFrames, SGraphs, SArrays, and Models are supported.")
    return obj

class GLPickler(_cloudpickle.CloudPickler):
    """

    # GLC pickle works with:
    #
    # (1) Regular python objects
    # (2) SArray
    # (3) SFrame
    # (4) SGraph
    # (5) Models
    # (6) Any combination of (1) - (5)
    
    Examples
    --------

    To pickle a collection of objects into a single file:
  
    .. sourcecode:: python

        from graphlab_util import gl_pickle
        import graphlab as gl
        
        obj = {'foo': gl.SFrame([1,2,3]),
               'bar': gl.SArray([1,2,3]),
               'foo-bar': ['foo-and-bar', gl.SFrame()]}
        
        # Setup the GLC pickler
        pickler = gl_pickle.GLPickler(filename = 'foo-bar')
        pickler.dump(obj)

        # The pickler has to be closed to make sure the files get closed.
        pickler.close()
        
    To unpickle the collection of objects:

    .. sourcecode:: python

        unpickler = gl_pickle.GLUnpickler(filename = 'foo-bar')
        obj = unpickler.load()
        print obj

    The GLC pickler needs a temporary working directory to manage GLC objects.
    This temporary working path must be a local path to the file system. It
    can also be a relative path in the FS.

    .. sourcecode:: python

        unpickler = gl_pickle.GLUnpickler('foo-bar', 
                                             gl_temp_storage_path = '/tmp')
        obj = unpickler.load()
        print obj


    Notes
    --------

    The GLC pickler saves the files into single zip archive with the following
    file layout.

    pickle_file_name: Name of the file in the archive that contains
                      the name of the pickle file. 
                      The comment in the ZipFile contains the version number 
                      of the GLC pickler used.

    "pickle_file": The pickle file that stores all python objects. For GLC objects
                   the pickle file contains a tuple with (ClassName, relative_path)
                   which stores the name of the GLC object type and a relative
                   path (in the zip archive) which points to the GLC archive
                   root directory.

    "gl_archive_dir_1" : A directory which is the GLC archive for a single
                          object.
 
     ....

    "gl_archive_dir_N" 
                          
 

    """
    def __init__(self, filename, protocol = -1, min_bytes_to_save = 0, 
                 gl_temp_storage_path = '/tmp'):
        """

        Construct a  GLC pickler.

        Parameters
        ----------
        filename  : Name of the file to write to. This file is all you need to pickle
                    all objects (including GLC objects).

        protocol  : Pickle protocol (see pickle docs). Note that all pickle protocols
                    may not be compatable with GLC objects.

        min_bytes_to_save : Cloud pickle option (see cloud pickle docs)

        gl_temp_storage_path : Temporary storage for all GLC objects. The path
                                may be a relative path or an absolute path.

        Returns
        ----------
        GLC pickler.

        """

        # Need a temp storage path for GLC objects.
        self.gl_temp_storage_path = _os.path.abspath(gl_temp_storage_path)
        self.gl_object_memo = set()
        if not _os.path.exists(self.gl_temp_storage_path):
            raise RuntimeError('%s is not a valid path.' 
                                        % self.gl_temp_storage_path)

        if _gl.deploy._predictive_service._file_util.is_s3_path(filename):
            self.archive_filename = _get_temp_filename()
            self.s3_path = filename
            self.hdfs_path = None
        elif _gl.deploy._internal_utils.is_hdfs_url(filename):
            self.archive_filename = _get_temp_filename()
            self.s3_path = None
            self.hdfs_path = filename
            self.hadoop_conf_dir = None
        else:
            abs_file = _os.path.abspath(filename)
            dir_name = _os.path.dirname(abs_file)
            if not _os.path.exists(dir_name):
                print "Directory %s does not exist. Creating..." % dir_name
                _os.makedirs(dir_name)

            self.archive_filename = abs_file
            self.s3_path = None
            self.hdfs_path = None
            self.hadoop_conf_dir = None

        # Chose a random file name to save the pickle contents.
        relative_pickle_filename = str(_uuid.uuid4())
        pickle_filename = _os.path.join(self.gl_temp_storage_path, 
                                                    relative_pickle_filename)
        try:
            # Initialize the pickle file with cloud _pickle. Note, cloud pickle
            # takes a file handle for intializzation (unlike GLC pickle which takes
            # a file name)
            self.file = open(pickle_filename, 'wb')
            _cloudpickle.CloudPickler.__init__(self, self.file, protocol)
        except IOError as err:
            print "GraphLab create pickling error: %s" % err

        # Save the name of the pickle file and the version number of the 
        # GLC pickler.
        zf = _zipfile.ZipFile(self.archive_filename, 'w') 
        try:
            info = _zipfile.ZipInfo('pickle_file')
            info.comment = "1.0" # Version
            zf.writestr(info, relative_pickle_filename)
        except IOError as err:
            print "GraphLab create pickling error: %s" % err
            self.file.close()
        finally:
            zf.close()

    def _set_hdfs_exec_dir(self, exec_dir):
        self.hdfs_exec_dir= exec_dir


    def dump(self, obj):
        _cloudpickle.CloudPickler.dump(self, obj)

        
    def persistent_id(self, obj):
        """
        Provide a persistant ID for "saving" GLC objects by reference. Return
        None for all non GLC objects.

        Parameters
        ----------

        obj: Name of the object whose persistant ID is extracted.

        Returns
        --------
        None if the object is not a GLC object. (ClassName, relative path)
        if the object is a GLC object.

        Notes
        -----

        Borrowed from pickle docs (https://docs.python.org/2/library/_pickle.html)

        For the benefit of object persistence, the pickle module supports the
        notion of a reference to an object outside the pickled data stream.

        To pickle objects that have an external persistent id, the pickler must
        have a custom persistent_id() method that takes an object as an argument and
        returns either None or the persistent id for that object. 

        For GLC objects, the persistent_id is merely a relative file path (within
        the ZIP archive) to the GLC archive where the GLC object is saved. For
        example:
    
            (SFrame, 'sframe-save-path')
            (SGraph, 'sgraph-save-path')
            (Model, 'model-save-path')

        """

        # Get the class of the object (if it can be done)
        obj_class = None if not hasattr(obj, '__class__') else obj.__class__
        if obj_class is None:
            return None

        # If the object is a GLC class.
        if _is_not_pickle_safe_gl_class(obj_class):
            if (id(obj) in self.gl_object_memo):
                # has already been pickled
                return (None, None, id(obj)) 
            else:
                # Save the location of the GLC object's archive to the pickle file.
                relative_filename = str(_uuid.uuid4())
                filename = _os.path.join(self.gl_temp_storage_path, relative_filename)

                # Save the GLC object and then write to a zip archive
                obj.save(filename)
                for abs_name in _glob.glob(_os.path.join(filename, '*')):
                    zf = _zipfile.ZipFile(self.archive_filename, mode='a')
                    rel_name = _os.path.relpath(abs_name, self.gl_temp_storage_path)
                    zf.write(abs_name, rel_name)
                    zf.close()
                self.gl_object_memo.add(id(obj))
                # Return the tuple (class_name, relative_filename) in archive. 
                return (_get_gl_class_type(obj.__class__), relative_filename, id(obj)) 

        # Not a GLC object. Default to cloud pickle
        else:
            return None

    def close(self):
        """
        Close the pickle file, and the zip archive file. The single zip archive
        file can now be shipped around to be loaded by the unpickler.
        """
        # Close the pickle file.
        self.file.close()

        # Add the pickle file name to the zip file archive.
        zf = _zipfile.ZipFile(self.archive_filename, mode='a')
        try:
            abs_name = self.file.name
            rel_name = _os.path.relpath(abs_name, self.gl_temp_storage_path)
            zf.write(abs_name, rel_name)
        except: 
            raise IOError("GraphLab pickling error: Unable to add the pickle"
                          " file to the archive.")
        finally:
            zf.close()

        if self.s3_path:
            _gl.deploy._predictive_service._file_util.upload_to_s3(self.archive_filename,
                                                                   self.s3_path, {})

        if self.hdfs_path:
            _gl.deploy._internal_utils.upload_to_hdfs(self.archive_filename, self.hdfs_path, self.hadoop_conf_dir)


class GLUnpickler(_pickle.Unpickler):
    """
    # GLC unpickler works with a GLC pickler archive or a regular pickle 
    # archive.
    #
    # Works with 
    # (1) GLPickler archive 
    # (2) Cloudpickle archive
    # (3) Python pickle archive

    Examples
    --------
    To unpickle the collection of objects:

    .. sourcecode:: python

        unpickler = gl_pickle.GLUnpickler('foo-bar')
        obj = unpickler.load()
        print obj
                          
    """

    def __init__(self, filename, gl_temp_storage_path = '/tmp/'):
        """
        Construct a GLC unpickler.

        Parameters
        ----------
        filename  : Name of the file to read from. The file can be a GLC pickle
                    file, a cloud pickle file, or a python pickle file.

        gl_temp_storage_path : Temporary storage for all GLC objects. The path
                               may be a relative path or an absolute path.

        Returns
        ----------
        GLC unpickler.
        """
        self.gl_object_memo = {}
        if _gl.deploy._predictive_service._file_util.is_s3_path(filename):
            tmp_file = _get_temp_filename()
            _gl.deploy._predictive_service._file_util.download_from_s3(filename, tmp_file)
            filename = tmp_file
        if _gl.deploy._internal_utils.is_hdfs_url(filename):
            tmp_file = _get_temp_filename()
            _gl.deploy._internal_utils.download_from_hdfs(filename, tmp_file)
            filename = tmp_file

        # Need a temp storage path for GLC objects.
        self.gl_temp_storage_path = _os.path.abspath(gl_temp_storage_path)
        if not _os.path.exists(self.gl_temp_storage_path):
            raise RuntimeError('%s is not a valid path.' 
                                        % self.gl_temp_storage_path)

        # Check that the archive file is valid.
        if not _os.path.exists(filename):
            raise RuntimeError('%s is not a valid file name.' 
                                        % filename)

        # If the file is a Zip archive, then it will try to use the GLC 
        # unpickler, else it will use regular unpickler.
        if _zipfile.is_zipfile(filename):

            pickle_filename = None

            # Get the pickle file name.
            zf = _zipfile.ZipFile(filename)
            for info in zf.infolist():
                if info.filename == 'pickle_file':
                    pickle_filename = zf.read(info.filename)
            if pickle_filename is None:
                raise IOError(("Cannot pickle file of the given format. File"
                        " must be one of (a) GLPickler archive, "
                        "(b) Cloudpickle archive, or (c) python pickle archive."))
        
            # Extract the files ihe zip archive
            try:
                outpath = self.gl_temp_storage_path
                zf.extractall(outpath)
            except IOError as err:
                print "Graphlab pickle extraction error: %s " % err

            # Open the pickle file
            abs_pickle_filename = _os.path.join(self.gl_temp_storage_path, pickle_filename)
            _pickle.Unpickler.__init__(self, open(abs_pickle_filename, 'rb'))

        # Not an archived pickle file. Use _pickle.Unpickler to do 
        # everything.
        else: 
            _pickle.Unpickler.__init__(self, open(filename, 'rb'))


    def persistent_load(self, pid):
        """
        Reconstruct a GLC object using the persistent ID.

        This method should not be used externally. It is required by the unpickler super class.

        Parameters
        ----------
        pid      : The persistent ID used in pickle file to save the GLC object.

        Returns
        ----------
        The GLC object.
        """
        if len(pid) == 2:
            # Pre GLC-1.3 release behavior, without memoization
            type_tag, filename = pid
            abs_path = _os.path.join(self.gl_temp_storage_path, filename) 
            return  _get_gl_object_from_persistent_id(type_tag, abs_path)
        else:
            # Post GLC-1.3 release behavior, with memoization
            type_tag, filename, object_id = pid
            if object_id in self.gl_object_memo:
                return self.gl_object_memo[object_id]
            else:
                abs_path = _os.path.join(self.gl_temp_storage_path, filename) 
                obj = _get_gl_object_from_persistent_id(type_tag, abs_path)
                self.gl_object_memo[object_id] = obj
                return obj

