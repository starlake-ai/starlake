call ..\env.cmd %1

SET COMET_ENV=FS
SET SPARK_DRIVER_MEMORY=1G
SET COMET_FS=file://
SET COMET_ROOT=%~dp0quickstart
SET COMET_METRICS_ACTIVE=true
SET COMET_ASSERTIONS_ACTIVE=true
SET COMET_SINK_TO_FILE=true
SET COMET_ANALYZE=false
SET COMET_HIVE=false
SET COMET_GROUPED=false
SET COMET_METRICS_PATH=/tmp/metrics/{domain}
SET SPARK_CONF_OPTIONS=--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file://%SPARK_DIR%\conf\log4j.properties.template

SET COMET_SCRIPT=%SPARK_SUBMIT% %SPARK_CONF_OPTIONS% --class com.ebiznext.comet.job.Main %COMET_JAR_FULL_NAME%
