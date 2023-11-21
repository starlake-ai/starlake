MACHINE="$(uname -m)"
echo Running on ${MACHINE}

if [ "$1" != "" ]
then
  MACHINE=$1
fi

echo Building for linux/$MACHINE
export cluster=$(gcloud config get-value container/cluster 2> /dev/null)
export zone=$(gcloud config get-value compute/zone 2> /dev/null)
export project=$(gcloud config get-value core/project 2> /dev/null)

docker buildx build --platform linux/$MACHINE -t europe-west1-docker.pkg.dev/$project/starlake-docker-repo/starlake-amd64:latest . --push
