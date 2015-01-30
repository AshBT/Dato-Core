Graphlab Create
===============

Introduction
------------

GraphLab Create is a machine learning platform that enables data scientists and app developers to easily create intelligent apps at scale. Building an intelligent, predictive application involves iterating over multiple steps: cleaning the data, developing features, training a model, and creating and maintaining a predictive service. GraphLab Create™ does all of this in one platform. It is easy to use, fast, and powerful.
 
For more details on the GraphLab Create see http://dato.com, including
documentation, tutorial, etc.

License
-------
GNU Affero General Public License. See [license](LICENSE).


Dependencies
------------

GraphLab Create now automatically satisfied most dependencies. 
There are however, a few dependencies which we cannot easily satisfy:

* On OS X: Apple clang 4.0 (LLVM 3.1) [Required]
  +  Required for compiling GraphLab Create.

* On Linux: g++ (>= 4.7) or clang (>= 3.1) [Required]
  +  Required for compiling GraphLab Create.

* *nix build tools: patch, make [Required]
   +  Should come with most Mac/Linux systems by default. Recent Ubuntu version will require to install the build-essential package.

* zlib [Required]
   +   Comes with most Mac/Linux systems by default. Recent Ubuntu version will require the zlib1g-dev package.

* Open MPI or MPICH2 [Strongly Recommended]
   + Required for running GraphLab distributed. 

* JDK 6 or greater [Optional]
   + Required for HDFS support 


    
### Satisfying Dependencies on Mac OS X

Installing XCode with the command line tools (in XCode >= 4.3 you have to do this
manually in the XCode Preferences -> Download pane), satisfies all of these
dependencies.  



### Satisfying Dependencies on Ubuntu

In Ubuntu >= 12.10, you can satisfy the dependencies with:
All the dependencies can be satisfied from the repository:

    sudo apt-get install gcc g++ build-essential libopenmpi-dev default-jdk cmake zlib1g-dev 

For Ubuntu versions prior to 12.10, you will need to install a newer version of gcc

    sudo add-apt-repository ppa:ubuntu-toolchain-r/test
    sudo apt-get update
    sudo apt-get install gcc-4.8 g++-4.8 build-essential libopenmpi-dev default-jdk cmake zlib1g-dev
    #redirect gcc to point to gcc-4.8
    sudo rm /usr/bin/gcc
    sudo ln -s /usr/bin/gcc-4.8 /usr/bin/gcc
    #redirect g++ to point to g++-4.8
    sudo rm /usr/bin/g++
    sudo ln -s /usr/bin/g++-4.8 /usr/bin/g++

    
Compiling
---------

    ./configure

Running configure will create two sub-directories, release/ and
debug/ . cd into src/unity/ under either of these directories and running make will build the
release or the debug versions respectively. 

We recommend using make’s parallel build feature to accelerate the compilation
process. For instance:

    make -j 4

will perform up to 4 build tasks in parallel. When building in release/ mode,
GraphLab Create does require a large amount of memory to compile with the
heaviest toolkit requiring 1GB of RAM. Where K is the amount of memory you
have on your machine in GB, we recommend not exceeding make -j K

To generate the python, cd one directory further into the python directory and run make again.

To use your dev build export these environment variables:
export PYTHONPATH="<repo root>/debug/src/unity/python/"
export GRAPHLAB_UNITY="<repo root>/debug/src/unity/server/unity_server"
where <repo root> is replace with the absolute path to the root of your repository.

Running Unit Tests
------------------

### Running Python unit tests
From the repo root:
cd debug/src/unity/python/graphlab/test
nosetests


### Running C++ units
From the repo root:
cd debug/test
make
ctest

  
Writing Your Own Apps
---------------------

See: https://github.com/graphlab-code/GraphLab-Create-SDK

