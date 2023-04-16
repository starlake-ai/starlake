MACHINE="$(uname -m)"
echo Running on ${MACHINE}

if [ "$1" != "" ]
then
  MACHINE=$1
fi

echo Building for linux/$MACHINE
docker buildx build --platform linux/$MACHINE -t starlake-$MACHINE .
