export SL_HOME=$(pwd)/../../distrib
export SL_ROOT=$(pwd)
export SL_METADATA=$(pwd)
#export SL_ENV=BQ

sh ${SL_HOME}/starlake.sh $@
