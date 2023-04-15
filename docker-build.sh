if [ "$1" == "m1" ]
then
  docker build -t starlake-m1 .
else
  docker buildx build --platform linux/amd64 -t starlake .
  export cluster=$(gcloud config get-value container/cluster 2> /dev/null)
  export zone=$(gcloud config get-value compute/zone 2> /dev/null)
  export project=$(gcloud config get-value core/project 2> /dev/null)

  docker tag starlake europe-west1-docker.pkg.dev/$project/starlake-docker-repo/starlake
  docker push europe-west1-docker.pkg.dev/$project/starlake-docker-repo/starlake
fi
