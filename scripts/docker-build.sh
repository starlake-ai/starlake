SCALA_VERSION=${SCALA_VERSION:-2.13}
MACHINE="$(uname -m)"
echo Running on ${MACHINE}

if [ "$1" != "" ]
then
  MACHINE=$1
fi


while true; do
    read -p "sbt clean publish ? " -n 1 -r
    echo
    case $REPLY in
        [Yy]* )
          sbt ++$SCALA_VERSION clean publish; break;;
        [Nn]* )
          echo
          break;;
        [Cc]* )
          echo
          exit;;
        * ) echo "Please answer yes or no or cancel.";;
    esac
done

echo Building for linux/$MACHINE
export cluster=$(gcloud config get-value container/cluster 2> /dev/null)
export zone=$(gcloud config get-value compute/zone 2> /dev/null)
export project=$(gcloud config get-value core/project 2> /dev/null)

docker buildx build  --platform linux/$MACHINE --build-arg SCALA_VERSION=$SCALA_VERSION --output type=docker -t starlake-ai/starlake-$MACHINE:latest .
#docker push europe-west1-docker.pkg.dev/$project/starlake-docker-repo/starlake-$MACHINE:latest
