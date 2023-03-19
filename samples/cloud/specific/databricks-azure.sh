COMET_SINK_TO_FILE=false
COMET_ASSERTIONS_ACTIVE=true
PYSPARK_PYTHON=/databricks/python3/bin/python3
COMET_GROUPED=false
COMET_METRICS_PATH=/mnt/starlake-app/tmp/quickstart/metrics/{{domain}}
COMET_METRICS_ACTIVE=true
COMET_ROOT=/mnt/starlake-app/tmp/quickstart
COMET_AUDIT_SINK_TYPE=BigQuerySink
COMET_FS=dbfs://
COMET_ANALYZE=false
COMET_ENV=BQ
COMET_HIVE=true
TEMPORARY_GCS_BUCKET=starlake-app

spark-submit
["--class", "ai.starlake.job.Main", "--jars", "/dbfs/mnt/starlake-app/jackson-dataformat-yaml-2.12.3.jar", "/dbfs/mnt/starlake-app/jackson-dataformat-yaml-2.12.3.jar", "import"]


bucket_name = "starlake-app"
mount_name = "starlake-app"
dbutils.fs.mount("gs://%s" % bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))



spark.hadoop.fs.azure.account.auth.type OAuth
spark.hadoop.fs.azure.account.oauth.provider.type org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
spark.hadoop.fs.azure.account.oauth2.client.id 507f8480-2ce0-4829-a9c1-1d69ed7db619
spark.hadoop.fs.azure.account.oauth2.client.secret IZY7Q~fv4eIxKMmBmUCIMxr-Wnfg8tynRhbQ5
spark.hadoop.fs.azure.account.oauth2.client.endpoint https://login.microsoftonline.com/d4bfb26e-8c71-425b-93e3-93a876109e43/oauth2/token

databricks personal access token dapi4a9eb215bcdfbec2fccdb4f84d8cf282

databricks secrets create-scope --scope starlake-scope --initial-manage-principal "users"
databricks secrets put --scope starlake-scope --key starlake-secret

configs = {
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "507f8480-2ce0-4829-a9c1-1d69ed7db619",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d4bfb26e-8c71-425b-93e3-93a876109e43/oauth2/token",
  "fs.azure.account.oauth2.client.secret": "IZY7Q~fv4eIxKMmBmUCIMxr-Wnfg8tynRhbQ5",
  "fs.azure.account.auth.type": "OAuth"
          }



bucket_name = "starlakecontainer@starlakesa.dfs.core.windows.net/"
mount_name = "starlake-app"
dbutils.fs.mount(source = "abfss://%s" % bucket_name, mount_point = "/mnt/%s" % mount_name,
  extra_configs = configs)

