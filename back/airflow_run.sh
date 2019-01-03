# To compute histogram add --conf spark.sql.statistics.histogram.enabled=true
export COMET_SPARK_CMD="/Users/hayssams/programs/spark-2.1.0-bin-hadoop2.7/bin/spark-submit --class com.ebiznext.comet.job.Main /Users/hayssams/git/comet/app/back/target/scala-2.11/comet-assembly-0.1.jar"
export COMET_DATASETS="/tmp/datasets"
export COMET_METADATA="/tmp/metadata"
export COMET_STAGING=true
export AIRFLOW_ENDPOINT="http://127.0.0.1:8080/api/experimental"
airflow $1
