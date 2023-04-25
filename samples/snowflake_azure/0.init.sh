source ./env.sh

#azcopy login --service-principal  --application-id application-id --tenant-id=d2770c15-b6c9-4df5-a35c-43b13fe9ecda
#azcopy copy "quickstart/" "https://$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$AZURE_STORAGE_CONTAINER/mnt/starlake-app/?sv=2021-12-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-12-31T22:24:00Z&st=2023-04-25T13:24:00Z&spr=https&sig=gXQ6g3nY531CXwo%2ButWlAFA5sk5rRSsUTu%2BnMmmjA9g%3D" --recursive=true


azcopy remove "https://$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$AZURE_STORAGE_CONTAINER/mnt/starlake-app/quickstart/?$AZURE_STORAGE_SAS" --recursive=true
azcopy copy "quickstart/" "https://$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$AZURE_STORAGE_CONTAINER/mnt/starlake-app/?$AZURE_STORAGE_SAS" --recursive=true
azcopy copy application.snowflake.conf "https://$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$AZURE_STORAGE_CONTAINER/mnt/starlake-app/quickstart/metadata/application.conf?$AZURE_STORAGE_SAS"







