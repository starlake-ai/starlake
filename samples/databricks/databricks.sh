COMET_SINK_TO_FILE=false
COMET_ASSERTIONS_ACTIVE=true
COMET_GROUPED=false
COMET_METRICS_PATH=/mnt/starlake-app-mnt/tmp/quickstart/metrics/{domain}
COMET_METRICS_ACTIVE=true
COMET_ROOT=/mnt/starlake-app-mnt/tmp/quickstart
COMET_AUDIT_SINK_TYPE=NoneSink
COMET_FS=dbfs://
COMET_ANALYZE=false
COMET_ENV=FS
COMET_HIVE=false


spark-submit
["--class", "ai.starlake.job.Main", "--jars", "/dbfs/mnt/starlake-app-mnt/jackson-dataformat-yaml-2.12.3.jar", "/dbfs/mnt/starlake-app-mnt/jackson-dataformat-yaml-2.12.3.jar", "import"]


bucket_name = "starlake-app"
mount_name = "starlake-app-mnt"
dbutils.fs.mount("gs://%s" % bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))
