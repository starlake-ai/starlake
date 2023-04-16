if [ "$1" == "m1" ]
then
  docker build -t --build-arg -t starlake-m1 .
else
  docker build --build-arg SL_VERSION=0.6.4 -t starlake .
fi

