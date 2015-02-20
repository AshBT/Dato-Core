#!/usr/bin/env python
'''
Copyright (C) 2015 Dato, Inc.
All rights reserved.

This software may be modified and distributed under the terms
of the BSD license. See the DATO-PYTHON-LICENSE file for details.
'''

import os
import sys
import glob
import subprocess
from setuptools import setup, find_packages
from setuptools.command.install import install

from graphlab_util.config import DEFAULT_CONFIG as CONFIG

PACKAGE_NAME="graphlab"

class InstallEngine(install):
    """Helper class to hook the python setup.py install path to download client libraries and engine"""

    def run(self):
        import platform

        # start by running base class implementation of run
        install.run(self)

        # Check correct version of architecture (64-bit only)
        arch = platform.architecture()[0]
        if arch != '64bit':
            msg = ("GraphLab Create currently supports only 64-bit operating systems, and only recent Linux/OSX " +
                   "architectures. Please install using a supported version. Your architecture is currently: %s" % arch)

            sys.stderr.write(msg)
            sys.exit(1)

        # Check correct version of Python
        if sys.version_info[:2] < (2, 7) or sys.version_info[0] > 2:
            msg = ("GraphLab Create currently only supports Python 2.7 and does not support any version of Python 3. " +
                   "Please install using a supported version. Your current Python version is: %s" % sys.version)
            sys.stderr.write(msg)
            sys.exit(1)

        # if OSX, verify > 10.7
        from distutils.util import get_platform
        cur_platform = get_platform()

        if cur_platform.startswith("macosx"):
            from pkg_resources import parse_version

            mac_ver = platform.mac_ver()[0]
            if parse_version(mac_ver) < parse_version('10.8.0'):
                msg = (
                "GraphLab Create currently does not support versions of OSX prior to 10.8. Please upgrade your Mac OSX "
                "installation to a supported version. Your current OSX version is: %s" % mac_ver)
                sys.stderr.write(msg)
                sys.exit(1)
        elif cur_platform.startswith('linux'):
            pass
        else:
            msg = (
                "Unsupported Platform: '%s'. GraphLab Create is only supported on Mac OSX and Linux." % cur_platform
            )
            sys.stderr.write(msg)
            sys.exit(1)

        print ""
        print ""
        print ""
        print "NOTE:"
        print ""
        print "Thank you for downloading and trying GraphLab Create."
        print ""
        print "GraphLab Create will send usage metrics to Dato, Inc. (when you import the graphlab module) to help us make GraphLab Create better. If you would rather these metrics are not collected, please remove GraphLab Create from your system."
        print ""
        print ""
        print ""
        if not CONFIG.version.endswith("gpu"):
            print "For Nvidia GPU CUDA support, please pip install http://static.dato.com/files/graphlab-create-" + CONFIG.version + ".gpu.tar.gz"

        from distutils import dir_util
        from distutils import file_util
        from distutils import sysconfig
        import stat
        import glob

        root_path = os.path.join(self.install_lib, PACKAGE_NAME)
        # copy the appropriate binaries and overwrite the dummy 0-byte files created
        if (cur_platform.startswith("macosx")):
            dir_util.copy_tree(os.path.join(root_path, "osx"), root_path)
        else:
            dir_util.copy_tree(os.path.join(root_path, "linux"), root_path)

        # move psutil files into place
        for psutil_file in glob.glob(os.path.join(root_path, '_psutil*.so')):
            file_util.move_file(psutil_file, os.path.join(root_path, '..', 'graphlab_psutil'))

        # make sure all of our binaries are executable to all
        packaged_binaries = ['unity_server', 'pylambda_worker', 'sftordd_pickle', 'rddtosf_pickle', 'rddtosf_nonpickle', 'spark_pipe_wrapper']
        packaged_binary_paths = []
        for exe_name in packaged_binaries:
            exe_path = os.path.join(root_path, exe_name)
            st = os.stat(exe_path)
            os.chmod(exe_path, st.st_mode | stat.S_IEXEC | stat.S_IXOTH | stat.S_IXGRP)
            packaged_binary_paths.append(exe_path)

        # write uuid to disk for mixpanel metrics
        import uuid

        with open(os.path.join(root_path, "id"), "w") as id_file:
            id_file.write(str(uuid.uuid4()))

        if (cur_platform.startswith("macosx")):
            # We need to do the following rpath modifications. First
            # find all the .so files and execute the following commaands on each .so file:
            #
            # - This removes the absolute path linking against the python framework
            #   and changes it to a relative path against @rpath
            # install_name_tool -change /System/Library/Frameworks/Python.framework/Versions/2.7/Python @rpath/libpython2.7.dylib
            # - This adds an rpath so that we know where to find libpython
            # install_name_tool -add_rpath LIBDIR
            # where LIBDIR = distutils.sysconfig.get_config_var('LIBDIR')
            so_glob = os.path.join(root_path, "cython", "*.so")
            # this appears to consistently tell me where is libpython or the python framework
            LIBDIR = sysconfig.get_config_var('LIBDIR')
            binaries_to_mod = glob.glob(so_glob)
            # Only add the binaries that need to search for libpython
            binaries_to_mod = binaries_to_mod + packaged_binary_paths[0:4]
            for so in binaries_to_mod:
                subprocess.call(
                    ["install_name_tool", "-change", "/System/Library/Frameworks/Python.framework/Versions/2.7/Python",
                     "@rpath/libpython2.7.dylib", so])
                subprocess.call(["install_name_tool", "-add_rpath", LIBDIR, so])
                subprocess.call(
                    ["install_name_tool", "-add_rpath", os.path.abspath(os.path.join(root_path, "cython")), so])


if __name__ == '__main__':
    setup(
        name="GraphLab-Create",
        version=CONFIG.version,
        author='Dato, Inc.',
        author_email='contact@dato.com',
        cmdclass=dict(install=InstallEngine),
        package_data={
        'graphlab': ['canvas/webapp/css/*.css', 'canvas/webapp/*.html', 'canvas/webapp/*.ico',
                     'canvas/webapp/js/*.js', 'canvas/webapp/images/*.png', 'cython/*.so', 'id',
                     'toolkits/deeplearning/*.conf',
                     'unity_server', 'pylambda_worker', 'rddtosf_nonpickle', 'sftordd_pickle',
                     'rddtosf_pickle', 'spark_pipe_wrapper', 'libhdfs.so', 'graphlab-create-spark-integration.jar', 'linux/cython/*.so',
                     'linux/pylambda_worker', 'linux/unity_server', 'linux/libhdfs.so',
                     'linux/sftordd_pickle', 'linux/rddtosf_nonpickle', 'linux/rddtosf_pickle', 'linux/spark_pipe_wrapper',
                     'linux/graphlab-create-spark-integration.jar',
                     'linux/_psutil*.so',
                     'osx/cython/*.so', 'osx/unity_server', 'osx/pylambda_worker',
                     'osx/rddtosf_nonpickle', 'osx/rddtosf_pickle', 'osx/spark_pipe_wrapper', 'osx/sftordd_pickle',
                     'osx/libhdfs.so', 'osx/graphlab-create-spark-integration.jar', 'osx/_psutil*.so', 'deploy/*.jar', 'lua/pl/*.lua',
                     'canvas/webapp/css/bootstrap/*.css',
                     'canvas/webapp/css/bootstrap/LICENSE',
                     'canvas/webapp/css/font-awesome/*.css',
                     'canvas/webapp/css/font-awesome/LICENSE',
                     'canvas/webapp/css/hljs/*.css',
                     'canvas/webapp/css/hljs/LICENSE',
                     'canvas/webapp/css/fonts/*.otf',
                     'canvas/webapp/css/fonts/*.eot',
                     'canvas/webapp/css/fonts/*.svg',
                     'canvas/webapp/css/fonts/*.ttf',
                     'canvas/webapp/css/fonts/*.woff',
                     'canvas/webapp/css/fonts/LICENSE',
                     'canvas/webapp/js/bootstrap/*.js',
                     'canvas/webapp/js/bootstrap/LICENSE',
                     'canvas/webapp/js/bowser/*.js',
                     'canvas/webapp/js/bowser/LICENSE',
                     'canvas/webapp/js/d3/*.js',
                     'canvas/webapp/js/d3/LICENSE',
                     'canvas/webapp/js/dagre/*.js',
                     'canvas/webapp/js/dagre/LICENSE',
                     'canvas/webapp/js/hljs/*.js',
                     'canvas/webapp/js/hljs/LICENSE',
                     'canvas/webapp/js/jquery/*.js',
                     'canvas/webapp/js/jquery/LICENSE',
                     'canvas/webapp/js/react/*.js',
                     'canvas/webapp/js/react/LICENSE',
                     'canvas/webapp/js/react-bootstrap/*.js',
                     'canvas/webapp/js/react-bootstrap/LICENSE',
                     'canvas/webapp/js/require/*.js',
                     'canvas/webapp/js/require/LICENSE',
                     'canvas/webapp/js/topojson/*.js',
                     'canvas/webapp/js/topojson/LICENSE',
                     'canvas/webapp/js/lodash/*.js',
                     'canvas/webapp/js/lodash/LICENSE'
                     ]},
        packages=find_packages(
            exclude=["*.tests", "*.tests.*", "tests.*", "tests", "*.test", "*.test.*", "test.*", "test",
                     "*.demo", "*.demo.*", "demo.*", "demo", "*.demo", "*.demo.*", "demo.*", "demo"]),
        url='https://dato.com',
        license='LICENSE.txt',
        description='GraphLab Create enables developers and data scientists to apply machine learning to build state of the art data products.',
        # long_description=open('README.txt').read(),
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Environment :: Console",
            "Intended Audience :: Developers",
            "Intended Audience :: Financial and Insurance Industry",
            "Intended Audience :: Information Technology",
            "Intended Audience :: Other Audience",
            "Intended Audience :: Science/Research",
            "License :: Other/Proprietary License",
            "Natural Language :: English",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: POSIX :: Linux",
            "Operating System :: POSIX :: BSD",
            "Operating System :: Unix",
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: Implementation :: CPython",
            "Topic :: Scientific/Engineering",
            "Topic :: Scientific/Engineering :: Information Analysis",
        ],
        install_requires=[
            "boto == 2.33.0",
            "librato-metrics == 0.4.9",
            "mixpanel-py == 3.1.1",
            "decorator == 3.4.0",
            "tornado == 3.2.1",
            "prettytable == 0.7.2",
            "requests == 2.3.0",
            "awscli == 1.6.2",
            "lockfile == 0.10.2"
        ],
    )
