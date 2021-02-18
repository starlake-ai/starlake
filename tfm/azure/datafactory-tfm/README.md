## Create Azure Service Account

az login

az account list --output table

az account set --subscription f2a37ffb-2cfb-4462-bf66-02cd34c15b15


az ad sp create-for-rbac --role="Contributor" --scopes="/subscriptions/f2a37ffb-2cfb-4462-bf66-02cd34c15b15"

RESOURCE_GROUP_NAME=kopicloud-tstate-rg
STORAGE_ACCOUNT_NAME=kopicloudtfstate$RANDOM
CONTAINER_NAME=tfstate
# Create resource group
az group create --name $RESOURCE_GROUP_NAME --location "West Europe"
# Create storage account
az storage account create --resource-group $RESOURCE_GROUP_NAME --name $STORAGE_ACCOUNT_NAME --sku Standard_LRS --encryption-services blob
# Get storage account key
ACCOUNT_KEY=$(az storage account keys list --resource-group $RESOURCE_GROUP_NAME --account-name $STORAGE_ACCOUNT_NAME --query [0].value -o tsv)
# Create blob container
az storage container create --name $CONTAINER_NAME --account-name $STORAGE_ACCOUNT_NAME --account-key $ACCOUNT_KEY
echo "storage_account_name: $STORAGE_ACCOUNT_NAME"
echo "container_name: $CONTAINER_NAME"
echo "access_key: $ACCOUNT_KEY"
