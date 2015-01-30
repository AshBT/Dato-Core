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
This module defines top level utility functions for GraphLab Create.
"""
import urllib as _urllib
import urllib2 as _urllib2
import sys as _sys
import os as _os
import re as _re
from zipfile import ZipFile as _ZipFile
import bz2 as _bz2
import tarfile as _tarfile
import ConfigParser as _ConfigParser
import itertools as _itertools

from graphlab.connect.aws._ec2 import get_credentials as _get_aws_credentials
import graphlab.connect as _mt
import graphlab.connect.main as _glconnect
import graphlab.connect.server as _server
import graphlab.version_info
import graphlab.sys_util as _sys_util
from pkg_resources import parse_version

import logging as _logging

__LOGGER__ = _logging.getLogger(__name__)

def _check_canvas_enabled(func):
    def error(*args, **kwargs):
        print 'This functionality is not available in GraphLab Create Open Source.' 

    # Is canvas available?
    try:
        import graphlab.canvas
    except ImportError:
        return error

    return func

def _try_inject_s3_credentials(url):
    """
    Inject aws credentials into s3 url as s3://[aws_id]:[aws_key]:[bucket/][objectkey]

    If s3 url already contains secret key/id pairs, just return as is.
    """
    assert url.startswith('s3://')
    path = url[5:]
    # Check if the path already contains credentials
    tokens = path.split(':')
    # If there are two ':', its possible that we have already injected credentials
    if len(tokens) == 3:
        # Edge case: there are exactly two ':'s in the object key which is a false alarm.
        # We prevent this by checking that '/' is not in the assumed key and id.
        if ('/' not in tokens[0]) and ('/' not in tokens[1]):
            return url

    # S3 url does not contain secret key/id pair, query the environment variables
    (k, v) = _get_aws_credentials()
    return 's3://' + k + ':' + v + ':' + path



def make_internal_url(url):
    """
    Takes a user input url string and translates into url relative to the server process.
    - URL to a local location begins with "local://" or has no "*://" modifier.
      If the server is local, returns the absolute path of the url.
      For example: "local:///tmp/foo" -> "/tmp/foo" and "./foo" -> os.path.abspath("./foo").
      If the server is not local, raise NotImplementedError.
    - URL to a server location begins with "remote://".
      Returns the absolute path after the "remote://" modifier.
      For example: "remote:///tmp/foo" -> "/tmp/foo".
    - URL to a s3 location begins with "s3://":
      Returns the s3 URL with credentials filled in using graphlab.aws.get_aws_credential().
      For example: "s3://mybucket/foo" -> "s3://$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY:mybucket/foo".
    - URL to other remote locations, e.g. http://, will remain as is.
    - Expands ~ to $HOME

    Parameters
    ----------
    string
        A URL (as described above).

    Raises
    ------
    ValueError
        If a bad url is provided.
    """
    if not url:
        raise ValueError('Invalid url: %s' % url)

    # The final file path on server.
    path_on_server = None

    # Try to split the url into (protocol, path).
    urlsplit = url.split("://")
    if len(urlsplit) == 2:
        protocol, path = urlsplit
        if not path:
            raise ValueError('Invalid url: %s' % url)
        if protocol in ['http', 'https']:
            # protocol is a remote url not on server, just return
            return url
        elif protocol == 'hdfs':
            if isinstance(_glconnect.get_server(), _server.LocalServer) and not _sys_util.get_hadoop_class_path():
                raise ValueError("HDFS URL is not supported because Hadoop not found. Please make hadoop available from PATH or set the environment variable HADOOP_HOME and try again.")
            else:
                return url
        elif protocol == 's3':
            return _try_inject_s3_credentials(url)
        elif protocol == 'remote':
        # url for files on the server
            path_on_server = path
        elif protocol == 'local':
        # url for files on local client, check if we are connecting to local server
            if (isinstance(_glconnect.get_server(), _server.LocalServer)):
                path_on_server = path
            else:
                raise ValueError('Cannot use local URL when connecting to a remote server.')
        else:
            raise ValueError('Invalid url protocol %s. Supported url protocols are: remote://, local://, s3://, https:// and hdfs://' % protocol)
    elif len(urlsplit) == 1:
        # expand ~ to $HOME
        url = _os.path.expanduser(url)
        # url for files on local client, check if we are connecting to local server
        if (isinstance(_glconnect.get_server(), _server.LocalServer)):
            path_on_server = url
        else:
            raise ValueError('Cannot use local URL when connecting to a remote server.')
    else:
        raise ValueError('Invalid url: %s' % url)

    if path_on_server:
        return _os.path.abspath(_os.path.expanduser(path_on_server))
    else:
        raise ValueError('Invalid url: %s' % url)


def download_dataset(url_str, extract=True, force=False, output_dir="."):
    """Download a remote dataset and extract the contents.

    Parameters
    ----------

    url_str : string
        The URL to download from

    extract : bool
        If true, tries to extract compressed file (zip/gz/bz2)

    force : bool
        If true, forces to retry the download even if the downloaded file already exists.

    output_dir : string
        The directory to dump the file. Defaults to current directory.
    """
    fname = output_dir + "/" + url_str.split("/")[-1]
    #download the file from the web
    if not _os.path.isfile(fname) or force:
        print "Downloading file from: ", url_str
        _urllib.urlretrieve(url_str, fname)
        if extract and fname[-3:] == "zip":
            print "Decompressing zip archive", fname
            _ZipFile(fname).extractall(output_dir)
        elif extract and fname[-6:] == ".tar.gz":
            print "Decompressing tar.gz archive", fname
            _tarfile.TarFile(fname).extractall(output_dir)
        elif extract and fname[-7:] == ".tar.bz2":
            print "Decompressing tar.bz2 archive", fname
            _tarfile.TarFile(fname).extractall(output_dir)
        elif extract and fname[-3:] == "bz2":
            print "Decompressing bz2 archive: ", fname
            outfile = open(fname.split(".bz2")[0], "w")
            print "Output file: ", outfile
            for line in _bz2.BZ2File(fname, "r"):
                outfile.write(line)
            outfile.close()
    else:
        print "File is already downloaded."


__GLCREATE_CURRENT_VERSION_URL__ = "https://dato.com/files/glcreate_current_version"

def get_newest_version(timeout=5, _url=__GLCREATE_CURRENT_VERSION_URL__):
    """
    Returns the version of GraphLab Create currently available from graphlab.com.
    Will raise an exception if we are unable to reach the graphlab.com servers.

    timeout: int
        How many seconds to wait for the remote server to respond

    url: string
        The URL to go to to check the current version.
    """
    request = _urllib2.urlopen(url=_url, timeout=timeout)
    version = request.read()
    __LOGGER__.debug("current_version read %s" % version)
    return version


def perform_version_check(configfile=(_os.path.join(_os.path.expanduser("~"), ".graphlab", "config")),
                          _url=__GLCREATE_CURRENT_VERSION_URL__,
                          _outputstream=_sys.stderr):
    """
    Checks if currently running version of GraphLab Create is less than the
    version available from graphlab.com. Prints a message if the graphlab.com
    servers are reachable, and the current version is out of date. Does 
    nothing otherwise.

    If the configfile contains a key "skip_version_check" in the Product
    section with non-zero value, this function does nothing.

    Also returns True if a message is printed, and returns False otherwise.
    """
    skip_version_check = False
    try:
        if (_os.path.isfile(configfile)):
            config = _ConfigParser.ConfigParser()
            config.read(configfile)
            section = 'Product'
            key = 'skip_version_check'
            skip_version_check = config.getboolean(section, key)
            __LOGGER__.debug("skip_version_check=%s" % str(skip_version_check))
    except:
        # eat all errors
        pass

    # skip version check set. Quit
    if not skip_version_check:
        try:
            latest_version = get_newest_version(timeout=1, _url=_url).strip()
            if parse_version(latest_version) > parse_version(graphlab.version_info.version):
                msg = ("A newer version of GraphLab Create (v%s) is available! "
                       "Your current version is v%s.\n"
                       "You can use pip to upgrade the graphlab-create package. "
                       "For more information see https://dato.com/products/create/upgrade.") % (latest_version, graphlab.version_info.version)
                _outputstream.write(msg)
                return True
        except:
            # eat all errors
            pass
    return False


def is_directory_archive(path):
    """
    Utiilty function that returns True if the path provided is a directory that has an SFrame or SGraph in it.

    SFrames are written to disk as a directory archive, this function identifies if a given directory is an archive
    for an SFrame.

    Parameters
    ----------
    path : string
        Directory to evaluate.

    Returns
    -------
    True if path provided is an archive location, False otherwise
    """
    if path is None:
        return False

    if not _os.path.isdir(path):
        return False

    ini_path = _os.path.join(path, 'dir_archive.ini')

    if not _os.path.exists(ini_path):
        return False

    if _os.path.isfile(ini_path):
        return True

    return False


def get_archive_type(path):
    """
    Returns the contents type for the provided archive path.

    Parameters
    ----------
    path : string
        Directory to evaluate.

    Returns
    -------
    Returns a string of: sframe, sgraph, raises TypeError for anything else
    """
    if not is_directory_archive(path):
        raise TypeError('Unable to determine the type of archive at path: %s' % path)

    try:
        ini_path = _os.path.join(path, 'dir_archive.ini')
        parser = _ConfigParser.SafeConfigParser()
        parser.read(ini_path)

        contents = parser.get('metadata', 'contents')
        return contents
    except Exception as e:
        raise TypeError('Unable to determine type of archive for path: %s' % path, e)

def get_environment_config():
    """
    Returns all the GraphLab Create configuration variables that can only
    be set via environment variables.

    GRAPHLAB_FILEIO_WRITER_BUFFER_SIZE
      The file write buffer size.

    GRAPHLAB_FILEIO_READER_BUFFER_SIZE
      The file read buffer size.

    OMP_NUM_THREADS
      The maximum number of threads to use for parallel processing.

    Parameters
    ----------
    None

    Returns
    -------
    Returns a dictionary of {key:value,..}
    """
    unity = _glconnect.get_unity()
    return unity.list_globals(False)

def get_runtime_config():
    """
    Returns all the GraphLab Create configuration variables that can be set
    at runtime. See :py:func:`graphlab.set_runtime_config()` to set these
    values and for documentation on the effect of each variable.

    Parameters
    ----------
    None

    Returns
    -------
    Returns a dictionary of {key:value,..}
    """
    unity = _glconnect.get_unity()
    return unity.list_globals(True)

def set_runtime_config(name, value):
    """
    Sets a runtime configuration value. These configuration values are also
    read from environment variables at program startup if available. See
    :py:func:`graphlab.get_runtime_config()` to get the current values for
    each variable.

    The default configuration is conservatively defined for machines with about
    4-8GB of RAM.

    *Basic Configuration Variables*

    GRAPHLAB_CACHE_FILE_LOCATIONS:
      The directory in which intermediate SFrames/SArray are stored.
      For instance "/var/tmp".  Multiple directories can be specified separated
      by a colon (ex: "/var/tmp:/tmp") in which case intermediate SFrames will
      be striped across both directories (useful for specifying multiple disks).
      Defaults to /var/tmp if the directory exists, /tmp otherwise.

    GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY:
      The maximum amount of memory which will be occupied by *all* intermediate
      SFrames/SArrays. Once this limit is exceeded, SFrames/SArrays will be
      flushed out to temporary storage (as specified by
      GRAPHLAB_CACHE_FILE_LOCATIONS). On large systems increasing this as well
      as GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY_PER_FILE can improve performance
      significantly. Defaults to 2147483648 bytes (2GB).

    GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY_PER_FILE:
      The maximum amount of memory which will be occupied by any individual
      intermediate SFrame/SArray. Once this limit is exceeded, the
      SFrame/SArray will be flushed out to temporary storage (as specified by
      GRAPHLAB_CACHE_FILE_LOCATIONS). On large systems, increasing this as well
      as GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY can improve performance
      significantly for large SFrames. Defaults to 134217728 bytes (128MB).

    *Advanced Configuration Variables*

    GRAPHLAB_SFRAME_FILE_HANDLE_POOL_SIZE:
      The maximum number of file handles to use when reading SFrames/SArrays.
      Once this limit is exceeded, file handles will be recycled, reducing
      performance. This limit should be rarely approached by most SFrame/SArray
      operations. Large SGraphs however may create a large a number of SFrames
      in which case increasing this limit may improve performance (You may
      also need to increase the system file handle limit with "ulimit -n").
      Defaults to 128.

    GRAPHLAB_SFRAME_IO_READ_LOCK
      Whether disk reads should be locked. Almost always necessary for magnetic
      disks for consistent performance. Can be disabled on SSDs. Defaults to
      True.

    GRAPHLAB_SFRAME_DEFAULT_BLOCK_SIZE
      The block size used by the SFrame file format. Increasing this will
      increase throughput of single SArray accesses, but decrease throughput of
      wide SFrame accesses. Defaults to 65536 bytes.

    ----------
    name: A string referring to runtime configuration variable.

    value: The value to set the variable to.

    Returns
    -------
    Nothing

    Raises
    ------
    A RuntimeError if the key does not exist, or if the value cannot be
    changed to the requested value.

    """
    unity = _glconnect.get_unity()
    ret = unity.set_global(name, value)
    if ret != "":
        raise RuntimeError(ret);

GLOB_RE = _re.compile("""[*?]""")
def split_path_elements(url):
    parts = _os.path.split(url)
    m = GLOB_RE.search(parts[-1])
    if m:
        return (parts[0], parts[1])
    else:
        return (url, "")

def crossproduct(d):
    """
    Create an SFrame containing the crossproduct of all provided options.

    Parameters
    ----------
    d : dict
        Each key is the name of an option, and each value is a list
        of the possible values for that option.

    Returns
    -------
    out : SFrame
        There will be a column for each key in the provided dictionary,
        and a row for each unique combination of all values.

    Example
    -------
    settings = {'argument_1':[0, 1],
                'argument_2':['a', 'b', 'c']}
    print crossproduct(settings)
    +------------+------------+
    | argument_2 | argument_1 |
    +------------+------------+
    |     a      |     0      |
    |     a      |     1      |
    |     b      |     0      |
    |     b      |     1      |
    |     c      |     0      |
    |     c      |     1      |
    +------------+------------+
    [6 rows x 2 columns]
    """

    _mt._get_metric_tracker().track('util.crossproduct')
    from graphlab import SArray
    d = [zip(d.keys(), x) for x in _itertools.product(*d.values())]
    sa = [{k:v for (k,v) in x} for x in d]
    return SArray(sa).unpack(column_name_prefix='')


def get_graphlab_object_type(url):
    '''
    Given url where a GraphLab Create object is persisted, return the GraphLab
    Create object type: 'model', 'graph', 'sframe', or 'sarray'
    '''
    ret = _glconnect.get_unity().get_graphlab_object_type(make_internal_url(url))

    # to be consistent, we use sgraph instead of graph here
    if ret == 'graph':
        ret = 'sgraph'
    return ret

class _distances_utility(object):
    """
    A collection of distance functions.

    Examples
    --------

    >>> import graphlab as gl
    >>> a = [1, 2, 3]
    >>> b = [4, 5, 6]
    >>> gl.distances.euclidean(a, b)

    """
    def __init__(self):
        distances = \
          {'euclidean':         lambda x, y: graphlab.extensions._distances.euclidean(x, y),
           'squared_euclidean': lambda x, y: graphlab.extensions._distances.squared_euclidean(x, y),
           'manhattan':         lambda x, y: graphlab.extensions._distances.manhattan(x, y),
           'cosine':            lambda x, y: graphlab.extensions._distances.cosine(x, y),
           'dot_product':       lambda x, y: graphlab.extensions._distances.dot_product(x, y),
           'levenshtein':       lambda x, y: graphlab.extensions._distances.levenshtein(x, y),
           'jaccard':           lambda x, y: graphlab.extensions._distances.jaccard(x, y),
           'weighted_jaccard':  lambda x, y: graphlab.extensions._distances.weighted_jaccard(x, y)
          }

        # TODO: It would be better to grab these from the __doc__ string that
        # is associated with the extension, but that would require an engine start.
        docstrings = {}

        docstrings['euclidean'] = \
            """
            Compute the Euclidean distance between two dictionaries or two lists
            of equal length. Suppose `x` and `y` each contain :math:`d`
            variables:

            .. math:: D(x, y) = \\sqrt{\sum_i^d (x_i - y_i)^2}

            Parameters
            ----------
            x : dict or list
                First input vector.

            y : dict or list
                Second input vector.

            Returns
            -------
            out : float
                Euclidean distance between `x` and `y`.

            Notes
            -----
            - If the input vectors are in dictionary form, keys missing in one
              of the two dictionaries are assumed to have value 0.

            References
            ----------
            - `Wikipedia - Euclidean distance
              <http://en.wikipedia.org/wiki/Euclidean_distance>`_

            Examples
            --------
            >>> gl.distances.euclidean([1, 2, 3], [4, 5, 6])
            5.196152422706632
            ...
            >>> gl.distances.euclidean({'a': 2, 'c': 4}, {'b': 3, 'c': 12})
            8.774964387392123
            """

        docstrings['squared_euclidean'] = \
            """
            Compute the squared Euclidean distance between two dictionaries or
            two lists of equal length. Suppose `x` and `y` each contain
            :math:`d` variables:

            .. math:: D(x, y) = \sum_i^d (x_i - y_i)^2

            Parameters
            ----------
            x : dict or list
                First input vector.

            y : dict or list
                Second input vector.

            Returns
            -------
            out : float
                Squared Euclidean distance between `x` and `y`.

            Notes
            -----
            - If the input vectors are in dictionary form, keys missing in one
              of the two dictionaries are assumed to have value 0.

            - Squared Euclidean distance does not satisfy the triangle
              inequality, so it is not a metric. This means the ball tree cannot
              be used to compute nearest neighbors based on this distance.

            References
            ----------
            - `Wikipedia - Euclidean distance
              <http://en.wikipedia.org/wiki/Euclidean_distance>`_

            Examples
            --------
            >>> gl.distances.squared_euclidean([1, 2, 3], [4, 5, 6])
            27.0
            ...
            >>> gl.distances.squared_euclidean({'a': 2, 'c': 4},
            ...                                {'b': 3, 'c': 12})
            77.0
            """

        docstrings['manhattan'] = \
            """
            Compute the Manhattan distance between between two dictionaries or
            two lists of equal length. Suppose `x` and `y` each contain
            :math:`d` variables:

            .. math:: D(x, y) = \\sum_i^d |x_i - y_i|

            Parameters
            ----------
            x : dict or list
                First input vector.

            y : dict or list
                Second input vector.

            Returns
            -------
            out : float
                Manhattan distance between `x` and `y`.

            Notes
            -----
            - If the input vectors are in dictionary form, keys missing in one
              of the two dictionaries are assumed to have value 0.

            - Manhattan distance is also known as "city block" or "taxi cab"
              distance.

            References
            ----------
            - `Wikipedia - taxicab geometry
              <http://en.wikipedia.org/wiki/Taxicab_geometry>`_

            Examples
            --------
            >>> gl.distances.manhattan([1, 2, 3], [4, 5, 6])
            9.0
            ...
            >>> gl.distances.manhattan({'a': 2, 'c': 4}, {'b': 3, 'c': 12})
            13.0
            """

        docstrings['cosine'] = \
            """
            Compute the cosine distance between between two dictionaries or two
            lists of equal length. Suppose `x` and `y` each contain
            :math:`d` variables:

            .. math::

                D(x, y) = 1 - \\frac{\sum_i^d x_i y_i}
                {\sqrt{\sum_i^d x_i^2}\sqrt{\sum_i^d y_i^2}}

            Parameters
            ----------
            x : dict or list
                First input vector.

            y : dict or list
                Second input vector.

            Returns
            -------
            out : float
                Cosine distance between `x` and `y`.

            Notes
            -----
            - If the input vectors are in dictionary form, keys missing in one
              of the two dictionaries are assumed to have value 0.

            - Cosine distance is not a metric. This means the ball tree cannot
              be used to compute nearest neighbors based on this distance.

            References
            ----------
            - `Wikipedia - cosine similarity
              <http://en.wikipedia.org/wiki/Cosine_similarity>`_

            Examples
            --------
            >>> gl.distances.cosine([1, 2, 3], [4, 5, 6])
            0.025368153802923787
            ...
            >>> gl.distances.cosine({'a': 2, 'c': 4}, {'b': 3, 'c': 12})
            0.13227816872537534
            """

        docstrings['dot_product'] = \
            """
            Compute the dot_product between two dictionaries or two lists of
            equal length. Suppose `x` and `y` each contain :math:`d` variables:

            .. math:: D(x, y) = \\frac{1}{\sum_i^d x_i y_i}

            Parameters
            ----------
            x : dict or list
                First input vector.

            y : dict or list
                Second input vector.

            Returns
            -------
            out : float

            Notes
            -----
            - If the input vectors are in dictionary form, keys missing in one
              of the two dictionaries are assumed to have value 0.

            - Dot product distance is not a metric. This means the ball tree
              cannot be used to compute nearest neighbors based on this
              distance.

            Examples
            --------
            >>> gl.distances.dot_product([1, 2, 3], [4, 5, 6])
            0.03125
            ...
            >>> gl.distances.dot_product({'a': 2, 'c': 4}, {'b': 3, 'c': 12})
            0.020833333333333332
            """

        docstrings['jaccard'] = \
            """
            Compute the Jaccard distance between between two dictionaries.
            Suppose :math:`K_x` and :math:`K_y` are the sets of keys from the
            two input dictionaries.

            .. math:: D(x, y) = 1 - \\frac{|K_x \cap K_y|}{|K_x \cup K_y|}

            Parameters
            ----------
            x : dict
                First input dictionary.

            y : dict
                Second input dictionary.

            Returns
            -------
            out : float
                Jaccard distance between `x` and `y`.

            Notes
            -----
            - Jaccard distance treats the keys in the input dictionaries as
              sets, and ignores the values in the input dictionaries.

            References
            ----------
            - `Wikipedia - Jaccard distance
              <http://en.wikipedia.org/wiki/Jaccard_index>`_

            Examples
            --------
            >>> gl.distances.jaccard({'a': 2, 'c': 4}, {'b': 3, 'c': 12})
            0.6666666666666667
            """

        docstrings['weighted_jaccard'] = \
            """
            Compute the weighted Jaccard distance between between two
            dictionaries. Suppose :math:`K_x` and :math:`K_y` are the sets of
            keys from the two input dictionaries, while :math:`x_k` and
            :math:`y_k` are the values associated with key :math:`k` in the
            respective dictionaries. Typically these values are counts, i.e. of
            words or n-grams.

            .. math::

                D(x, y) = 1 - \\frac{\sum_{k \in K_x \cup K_y} \min\{x_k, y_k\}}
                {\sum_{k \in K_x \cup K_y} \max\{x_k, y_k\}}

            Parameters
            ----------
            x : dict
                First input dictionary.

            y : dict
                Second input dictionary.

            Returns
            -------
            out : float
                Weighted jaccard distance between `x` and `y`.

            Notes
            -----
            - If a key is missing in one of the two dictionaries, it is assumed
              to have value 0.

            References
            ----------
            - Weighted Jaccard distance: Chierichetti, F., et al. (2010)
              `Finding the Jaccard Median
              <http://theory.stanford.edu/~sergei/papers/soda10-jaccard.pdf>`_.
              Proceedings of the Twenty-First Annual ACM-SIAM Symposium on
              Discrete Algorithms. Society for Industrial and Applied
              Mathematics.

            Examples
            --------
            >>> gl.distances.weighted_jaccard({'a': 2, 'c': 4},
            ...                               {'b': 3, 'c': 12})
            0.7647058823529411
            """

        docstrings['levenshtein'] = \
            """
            Compute the Levenshtein distance between between strings. The
            distance is the number of insertion, deletion, and substituion edits
            needed to transform string `x` into string `y`. The mathematical
            definition of Levenshtein is recursive:

           .. math::

                D(x, y) = d(|x|, |y|)

                d(i, j) = \max(i, j), \quad \mathrm{if } \min(i, j) = 0

                d(i, j) = \min \Big \{d(i-1, j) + 1, \ d(i, j-1) + 1, \ d(i-1, j-1) + I(x_i \\neq y_i) \Big \}, \quad \mathrm{else}


            Parameters
            ----------
            x : string
                First input string.

            y : string
                Second input string.

            Returns
            -------
            out : float
                Levenshtein distance between `x` and `y`.

            Notes
            -----

            References
            ----------
            - `Wikipedia - Levenshtein distance
              <http://en.wikipedia.org/wiki/Levenshtein_distance>`_

            Examples
            --------
            >>> gl.distances.levenshtein("fossa", "fossil")
            2.0
            """


        # Copy lambda along with its corresponding docstring
        self.__dict__ = {}
        for k in docstrings:
            d = distances[k]
            d.__doc__ = docstrings[k]
            self.__dict__[k] = d


def _get_distance(distance_name):
    """
    Get the actual function for a distance from the name of the distance.
    """
    def ismatch(distance, actual):
        return (distance == actual) or (distance == '_distances.'+actual)

    for distance, fn in graphlab.distances.__dict__.iteritems():
        if ismatch(distance_name, distance):
            return fn

    raise ValueError('Distance not recognized')

def _assert_sframe_equal(sf1,
                         sf2,
                         check_column_names=True,
                         check_column_order=True,
                         check_row_order=True):
    """
    Assert if the two SFrames are not equal.

    The default behavior of this function uses the strictest possible
    definition of equality, where all columns must be in the same order, with
    the same names and have the same data in the same order.  Each of these
    stipulations can be relaxed individually and in concert with another, with
    the exception of `check_column_order` and `check_column_names`, we must use
    one of these to determine which columns to compare with one another.

    This function does not attempt to apply a "close enough" definition to
    float values, and it is not recommended to rely on this function when
    SFrames may have a column of floats.

    Parameters
    ----------
    sf1 : SFrame

    sf2 : SFrame

    check_column_names : bool
        If true, assert if the data values in two columns are the same, but
        they have different names.  If False, column order is used to determine
        which columns to compare.

    check_column_order : bool
        If true, assert if the data values in two columns are the same, but are
        not in the same column position (one is the i-th column and the other
        is the j-th column, i != j).  If False, column names are used to
        determine which columns to compare.

    check_row_order : bool
        If true, assert if all rows in the first SFrame exist in the second
        SFrame, but they are not in the same order.
    """
    if (type(sf1) is not graphlab.SFrame) or (type(sf2) is not graphlab.SFrame):
        raise TypeError("Cannot function on types other than SFrames.")

    if not check_column_order and not check_column_names:
        raise ValueError("Cannot ignore both column order and column names.")

    sf1.__materialize__()
    sf2.__materialize__()

    if sf1.num_cols() != sf2.num_cols():
        raise AssertionError("Number of columns mismatched: " +
            str(sf1.num_cols()) + " != " + str(sf2.num_cols()))

    s1_names = sf1.column_names()
    s2_names = sf2.column_names()

    sorted_s1_names = sorted(s1_names)
    sorted_s2_names = sorted(s2_names)

    if check_column_names:
        if (check_column_order and (s1_names != s2_names)) or (sorted_s1_names != sorted_s2_names):
            raise AssertionError("SFrame does not have same column names: " +
                str(sf1.column_names()) + " != " + str(sf2.column_names()))

    if sf1.num_rows() != sf2.num_rows():
        raise AssertionError("Number of rows mismatched: " +
            str(sf1.num_rows()) + " != " + str(sf2.num_rows()))

    if not check_row_order and (sf1.num_rows() > 1):
        sf1 = sf1.sort(s1_names)
        sf2 = sf2.sort(s2_names)

    names_to_check = None
    if check_column_names:
      names_to_check = zip(sorted_s1_names, sorted_s2_names)
    else:
      names_to_check = zip(s1_names, s2_names)
    for i in names_to_check:
        if sf1[i[0]].dtype() != sf2[i[1]].dtype():
          raise AssertionError("Columns " + str(i) + " types mismatched.")
        if not (sf1[i[0]] == sf2[i[1]]).all():
            raise AssertionError("Columns " + str(i) + " are not equal!")
