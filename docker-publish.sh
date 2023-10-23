MACHINE="$(uname -m)"
echo Running on ${MACHINE}

if [ "$1" != "" ]
then
  MACHINE=$1
fi

export cluster=$(gcloud config get-value container/cluster 2> /dev/null)
export zone=$(gcloud config get-value compute/zone 2> /dev/null)
export project=$(gcloud config get-value core/project 2> /dev/null)

docker tag starlake-${MACHINE} europe-west1-docker.pkg.dev/$project/starlake-docker-repo/starlake-${MACHINE}
docker push europe-west1-docker.pkg.dev/$project/starlake-docker-repo/starlake-${MACHINE}


#az login
  #az acr login --name starlakecontainerregistry
#docker tag starlake-amd64:latest starlakecontainerregistry.azurecr.io/starlake-amd64:latest
#docker push starlakecontainerregistry.azurecr.io/starlake-amd64:latest
# TOKEN=$(az acr login --name starlakecontainerregistry --expose-token --output tsv --query accessToken)

