# Samples

## Extract
In the "extract" folder, you'll find a sample about how to:
- extract your database schema
- Generate the script to extract data from your database

## Load & Transform 
Each folder in the samples directory is based on a common quickstart example customized for a specific environment.
- local: If you develop locally and work on parquet files stored on your disk.
- local-bigquery: If you develop locally and publish your files in tables located in your currently active (gcloud config list 
  on your laptop will tell you which one  is active). Make sure your have sufficient rights on your Google CLoud project
- gcp: If you run your jobs from Cloud Composer in your Google CLoud project and create datasets in BigQuery
- hdfs: If you develop locally and work on parquet files stored on HDFS locally and / or remotely.
- hive: similar to the hdfs sample above but with parquet files available as Hive tables.

To run the samples:
- Import the raw datasets using the script ./1.import.sh
- Ingest the files into parquet / Hive / BigQuery / ... tables using ./2./watch.sh
- Finally compute the KPI using ./3.transform.sh

To display the results, start the Zeppelin notebook (requires Docker installed) and open the notebook named after the sample you ran. 

