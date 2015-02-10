
osname=""
if [[ "$OSTYPE" == "linux-gnu" ]]; then
        osname="linux"
elif [[ "$OSTYPE" == "darwin"* ]]; then
        osname="mac"
else
        echo "Unknown Operating System"
        exit
fi

make python_source

cd graphlab
rm -rf linux
rm -rf mac
cd ..

mkdir graphlab/linux
mkdir graphlab/mac

cp graphlab/cython/cy_dataframe.so graphlab/cython/cy_flexible_type.so graphlab/cython/cy_graph.so graphlab/cython/cy_ipc.so graphlab/cython/cy_model.so graphlab/cython/cy_sarray.so graphlab/cython/cy_sframe.so graphlab/cython/cy_sketch.so graphlab/cython/cy_test_utils.so graphlab/cython/cy_type_utils.so graphlab/cython/cy_unity.so graphlab/cython/cy_variant.so graphlab/cython/libbase_dep.so ../server/unity_server ../../lambda/pylambda_worker ../../sframe/rddtosf_nonpickle ../../sframe/rddtosf_pickle ../../sframe/sftordd_pickle ../../sframe/graphlab-create-spark-integration.jar graphlab_psutil/*.so graphlab/$osname/

touch graphlab/rddtosf_nonpickle
touch graphlab/rddtosf_pickle
touch graphlab/sftordd_pickle
touch graphlab/unity_server
touch graphlab/graphlab-create-spark-integration.jar
touch graphlab/pylambda_worker

cd graphlab_util
rm -f _psutil*
rm -f psutil*

cd ../graphlab_psutil
rm -f _psutil*
rm -f psutil*
touch _psutil_linux.so
touch _psutil_posix.so
touch _psutil_osx.so

cd ..

# Apply version string
sed -i s/{{VERSION_STRING}}/1.3-oss/g doc/source/conf.py graphlab/__init__.py graphlab/version_info.py
python setup.py sdist
