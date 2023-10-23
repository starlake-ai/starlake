




CREATE OR REPLACE STORAGE INTEGRATION 
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = 'd2770c15-b6c9-4df5-a35c-43b13fe9ecda'
  STORAGE_ALLOWED_LOCATIONS = ('azure://starlakestoraccnt.blob.core.windows.net/starlakecontainer/hr/','azure://starlakestoraccnt.blob.core.windows.net/starlakecontainer/sales/');
DESC STORAGE INTEGRATION ;
    
CREATE STAGE HR_STAGE
  URL = 'azure://starlakestoraccnt.blob.core.windows.net/starlakecontainer/hr/'
  STORAGE_INTEGRATION = ;
        
CREATE STAGE SALES_STAGE
  URL = 'azure://starlakestoraccnt.blob.core.windows.net/starlakecontainer/sales/'
  STORAGE_INTEGRATION = ;
        
CREATE NOTIFICATION INTEGRATION 
  ENABLED = true
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://STARLAKE.queue.core.windows.net/STARLAKE_QUEUE'
  AZURE_TENANT_ID = 'd2770c15-b6c9-4df5-a35c-43b13fe9ecda';
DESC NOTIFICATION INTEGRATION ;
        
CREATE OR REPLACE STORAGE INTEGRATION hr_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = 'd2770c15-b6c9-4df5-a35c-43b13fe9ecda'
  STORAGE_ALLOWED_LOCATIONS = ('azure://starlakestoraccnt.blob.core.windows.net/starlakecontainer/hr/');
DESC STORAGE INTEGRATION hr_integration;
        
CREATE OR REPLACE PIPE HR_SELLERS_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = ''
  AS
  COPY INTO sellers
  FROM @HR_STAGE
  FILE_FORMAT = (TYPE = 'JSON');
            
CREATE OR REPLACE PIPE HR_LOCATIONS_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = ''
  AS
  COPY INTO locations
  FROM @HR_STAGE
  FILE_FORMAT = (TYPE = 'JSON');
            
CREATE OR REPLACE STORAGE INTEGRATION sales_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = 'd2770c15-b6c9-4df5-a35c-43b13fe9ecda'
  STORAGE_ALLOWED_LOCATIONS = ('azure://starlakestoraccnt.blob.core.windows.net/starlakecontainer/sales/');
DESC STORAGE INTEGRATION sales_integration;
        
CREATE OR REPLACE PIPE SALES_CUSTOMERS_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = ''
  AS
  COPY INTO customers
  FROM @SALES_STAGE
  FILE_FORMAT = (TYPE = 'JSON');
            
CREATE OR REPLACE PIPE SALES_ORDERS_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = ''
  AS
  COPY INTO orders
  FROM @SALES_STAGE
  FILE_FORMAT = (TYPE = 'JSON');
            




/*

az login
az provider register --namespace Microsoft.EventGrid
az provider show --namespace Microsoft.EventGrid --query &quot;registrationState&quot;
az group create --name starlakerg --location francecentral
az storage account create --resource-group starlakerg --name STARLAKE_STORAGE_ACCOUNT --sku Standard_LRS --location francecentral --kind StorageV2 --access-tier Hot
az storage account create --resource-group starlakerg --name STARLAKE_STORAGE_ACCOUNT --sku Standard_LRS --location francecentral --kind StorageV2 --access-tier Hot
az storage account keys list --resource-group starlakerg --account-name STARLAKE_STORAGE_ACCOUNT
# Get key1 value (base64)
az storage queue create --account-key 000000== --account-name STARLAKE_STORAGE_ACCOUNT --name 

export storageid=$(az storage account show --name STARLAKE_STORAGE_ACCOUNT --resource-group starlakerg --query id --output tsv)
export queuestorageid=$(az storage account show --name STARLAKE_STORAGE_ACCOUNT --resource-group starlakerg --query id --output tsv)
export queueid=&quot;$queuestorageid/queueservices/default/queues/&quot;

az extension add --name eventgrid
az eventgrid event-subscription create --source-resource-id $storageid --name  --endpoint-type storagequeue --endpoint $queueid --advanced-filter data.api stringin CopyBlob PutBlob PutBlockList FlushWithClose SftpCommit

export queueurl=https://STARLAKE_STORAGE_ACCOUNT.queue.core.windows.net/
    
*/




alter session set TIMESTAMP_TYPE_MAPPING = 'TIMESTAMP_LTZ';
CREATE SCHEMA IF NOT EXISTS hr
COMMENT = '';


CREATE SCHEMA IF NOT EXISTS AUDIT;
CREATE TABLE IF NOT EXISTS AUDIT.AUDIT (
    jobid VARCHAR,
    paths VARCHAR,
    domain VARCHAR,
    schema VARCHAR,
    success BOOLEAN,
    count INTEGER,
    countAccepted INTEGER,
    countRejected INTEGER,
    timestamp TIMESTAMP,
    duration INTEGER,
    message VARCHAR,
    step VARCHAR
 );




CREATE TABLE IF NOT EXISTS hr.sellers (
        id STRING  NOT NULL comment '' ,
        seller_email STRING  NOT NULL comment '' ,
        location_id INTEGER  NOT NULL comment '' 
)
;






CREATE TABLE IF NOT EXISTS hr.locations (
        id STRING  NOT NULL comment '' ,
        address VARCHAR  NOT NULL comment '' 
)
;






alter session set TIMESTAMP_TYPE_MAPPING = 'TIMESTAMP_LTZ';
CREATE SCHEMA IF NOT EXISTS sales
COMMENT = '';


CREATE SCHEMA IF NOT EXISTS AUDIT;
CREATE TABLE IF NOT EXISTS AUDIT.AUDIT (
    jobid VARCHAR,
    paths VARCHAR,
    domain VARCHAR,
    schema VARCHAR,
    success BOOLEAN,
    count INTEGER,
    countAccepted INTEGER,
    countRejected INTEGER,
    timestamp TIMESTAMP,
    duration INTEGER,
    message VARCHAR,
    step VARCHAR
 );




CREATE TABLE IF NOT EXISTS sales.customers (
        id STRING  NOT NULL comment '' ,
        signup TIMESTAMP   comment '' ,
        contact STRING   comment '' ,
        birthdate DATE   comment '' ,
        firstname STRING   comment '' ,
        lastname STRING   comment '' 
)
;






CREATE TABLE IF NOT EXISTS sales.orders (
        id STRING  NOT NULL comment '' ,
        customer_id STRING   comment '' ,
        amount DECIMAL   comment '' ,
        seller_id STRING   comment '' 
)
;


