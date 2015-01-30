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
import sys
import os
import logging
import commands
from distutils.util import get_platform as _get_platform
import glob as _glob


def make_unity_server_env():
    """
    Returns the environemnt for unity_server.

    The environment is necessary to start the unity_server
    by setting the proper enviornments for shared libraries,
    hadoop classpath, and module search paths for python lambda workers.

    The environment has 3 components:
    1. CLASSPATH, contains hadoop class path
    2. LD_LIBRARY_PATH: contains path to shared libraries of jvm and libpython2.7. This is essential for unity_server to get started.
    3. __GL_SYS_PATH__: contains the python sys.path of the interpreter. This is essential for pylambda worker to behave properly.
    """
    env = os.environ.copy()

    # Add hadoop class path
    classpath = get_hadoop_class_path()
    if ("CLASSPATH" in env):
        env["CLASSPATH"] = env['CLASSPATH'] + (":" + classpath if classpath != '' else '')
    else:
        env["CLASSPATH"] = classpath

    # Add JVM path
    libjvm_path = get_libjvm_path()

    # Add libpython path
    libpython_path = get_libpython_path()
    if 'LD_LIBRARY_PATH' in env:
        env['LD_LIBRARY_PATH'] = env['LD_LIBRARY_PATH'] + ":" + libpython_path + (":" + libjvm_path if libjvm_path != '' else '')
    else:
        env['LD_LIBRARY_PATH'] = libpython_path + ":" + libjvm_path

    # Add python syspath
    env['__GL_SYS_PATH__'] = ':'.join(sys.path)

    #### Remove PYTHONEXECUTABLE ####
    # Anaconda overwrites this environment variable
    # which forces all python sub-processes to use the same binary.
    # When using virtualenv with ipython (which is outside virtualenv),
    # all subprocess launched under unity_server will use the
    # conda binary outside of virtualenv, which lacks the access
    # to all packeages installed inside virtualenv.
    if 'PYTHONEXECUTABLE' in env:
        del env['PYTHONEXECUTABLE']
    return env


def get_libpython_path():
    """
    Return the path of libpython2.7.so in the current python environment.

    The path is a guess based on the heuristics that most of the time the library
    is under sys.exec_prefix + "/lib".
    """
    libpython_path = sys.exec_prefix + "/lib"
    return libpython_path


def get_libjvm_path():
    """
    Return path of libjvm.so for LD_LIBRARY_PATH variable.
    This is only required for Linux platform, and by looking in a bunch of 'known' paths.
    """
    cur_platform = _get_platform()

    if cur_platform.startswith("macosx"):
        return ''
    else:
        potential_paths = [
                '/usr/lib/jvm/default-java/jre/lib/amd64/server',               # ubuntu / debian distros
                '/usr/lib/jvm/java/jre/lib/amd64/server',                       # rhel6
                '/usr/lib/jvm/jre/lib/amd64/server',                            # centos6
                '/usr/lib64/jvm/jre/lib/amd64/server',                          # opensuse 13
                '/usr/local/lib/jvm/default-java/jre/lib/amd64/server',         # alt ubuntu / debian distros
                '/usr/local/lib/jvm/java/jre/lib/amd64/server',                 # alt rhel6
                '/usr/local/lib/jvm/jre/lib/amd64/server',                      # alt centos6
                '/usr/local/lib64/jvm/jre/lib/amd64/server',                    # alt opensuse 13
                '/usr/local/lib/jvm/java-7-openjdk-amd64/jre/lib/amd64/server', # alt ubuntu / debian distros
                '/usr/lib/jvm/java-7-openjdk-amd64/jre/lib/amd64/server',       # alt ubuntu / debian distros
                '/usr/local/lib/jvm/java-6-openjdk-amd64/jre/lib/amd64/server', # alt ubuntu / debian distros
                '/usr/lib/jvm/java-6-openjdk-amd64/jre/lib/amd64/server',       # alt ubuntu / debian distros
                '/usr/lib/jvm/java-7-oracle/jre/lib/amd64/server',              # alt ubuntu
                '/usr/lib/jvm/java-8-oracle/jre/lib/amd64/server',              # alt ubuntu
                '/usr/lib/jvm/java-6-oracle/jre/lib/amd64/server',              # alt ubuntu
                '/usr/local/lib/jvm/java-7-oracle/jre/lib/amd64/server',        # alt ubuntu
                '/usr/local/lib/jvm/java-8-oracle/jre/lib/amd64/server',        # alt ubuntu
                '/usr/local/lib/jvm/java-6-oracle/jre/lib/amd64/server',        # alt ubuntu
                ]
        for path in potential_paths:
            if os.path.isfile(os.path.join(path, 'libjvm.so')):
                logging.getLogger(__name__).debug('libjvm.so path used: %s' % path)
                return path
        return ''


def get_hadoop_class_path():
    # Try get the classpath directly from executing hadoop
    try:
        status, output = commands.getstatusoutput('hadoop classpath')
        if status != 0:
            status, output = commands.getstatusoutput('${HADOOP_HOME}/bin/hadoop classpath')

        if status == 0:
            output = ':'.join(os.path.realpath(path) for path in output.split(':'))
            return _get_expanded_classpath(output)

    except Exception as e:
        logging.getLogger(__name__).warning("Exception trying to retrieve Hadoop classpath: %s" % e)

    logging.getLogger(__name__).debug("Hadoop not found. HDFS url is not supported. Please make hadoop available from PATH or set the environment variable HADOOP_HOME.")
    return ""


def _get_expanded_classpath(classpath):
    """
    Take a classpath of the form:
      /etc/hadoop/conf:/etc/hadoop/conf:/etc/hadoop/conf:/usr/lib/hadoop/lib/*:/usr/lib/hadoop/.//*: ...

    and return it expanded to all the JARs (and nothing else):
      /usr/lib/hadoop/lib/netty-3.6.2.Final.jar:/usr/lib/hadoop/lib/jaxb-api-2.2.2.jar: ...

    mentioned in the path
    """
    if classpath is None or classpath == '':
        return ''

    #  so this set comprehension takes paths that end with * to be globbed to find the jars, and then
    #  recombined back into a colon separated list of jar paths, removing dupes and using full file paths
    jars = ':'.join({":".join([os.path.abspath(jarpath) for jarpath in _glob.glob(path)])
                    for path in classpath.split(':') if path.endswith('*')})
    logging.getLogger(__name__).debug('classpath being used: %s' % jars)
    return jars
