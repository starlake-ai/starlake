# Samples

## Extract
In the "extract" folder, you'll find a sample about how to:
- extract your database schema
- Generate the script to extract data from your database

## Load & Transform 
Each folder in the samples directory is based on a common quickstart and show how to run in different context.
- local: If you develop locally and need to store files locally , in HDFS or Snowflake. This is a good start for developping on any JDBC compliant Database, 
Hadoop Cluster or for evaluation purposes.
- bigquery_gcp: Run from you local machine with ingestion configuration stored in GCP and ingest data into BigQuery on Google Cloud
- snowflake: Run from you local machine and ingest data into Snowflake
- snowflake_azure: Run from you local machine with ingestion configuration stored in Azure and ingest data into Snowflake

To run the any of the samples:
- First copy some files in the incoming folder: ./0.init.sh
- Import the raw datasets using the script ./1.import.sh
- Ingest the files into your datawarehouse using ./2.watch.sh
- Finally compute the KPI using ./3.transform.sh


