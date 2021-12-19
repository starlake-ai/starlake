COMET_SINK_TO_FILE=false
COMET_ASSERTIONS_ACTIVE=true
PYSPARK_PYTHON=/databricks/python3/bin/python3
COMET_GROUPED=false
COMET_METRICS_PATH=/mnt/starlake-app/tmp/quickstart/metrics/{domain}
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


spark.hadoop.google.cloud.auth.service.account.enable true
spark.hadoop.fs.gs.auth.service.account.email hayssam-saleh@dummy.iam.gserviceaccount.com  # beautiful dummy service account
spark.hadoop.fs.gs.project.id starlake-325712
spark.hadoop.fs.gs.auth.service.account.private.key -----BEGIN NON PRIVATE KEY----------END NON PRIVATE KEY-----\n
spark.hadoop.fs.gs.auth.service.account.private.key.id df728e47e5e6c14402fafe6d39a3b8792a9967c7 # beautiful dummy key

