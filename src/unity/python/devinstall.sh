# source this file (. devinstall.sh) to get exports in the right place
# then you can run graphlab from within the venv and it points to this
# development directory

# create a virtualenv (if necessary) and activate it
virtualenv venv
. venv/bin/activate

# set the export path for unity server
export GRAPHLAB_UNITY=$(pwd)/../server/unity_server

# install the package locally in development mode (so the files are symlinked and a make picks up new changes)
ARCHFLAGS=-Wno-error=unused-command-line-argument-hard-error-in-future pip install -e .

# install ipython notebook dependencies
ARCHFLAGS=-Wno-error=unused-command-line-argument-hard-error-in-future pip install "ipython[notebook]"
