PROG_DIR=$(cd `dirname $0` && pwd)

if [ "$PWD" != "$PROG_DIR" ]; then
  echo "run command from local folder in the form ./0.init.sh"
  exit 1
fi

if ls drivers/*.jar 1>/dev/null 2>&1; then
    echo "JAR in JDBC driver folfer found"
else
    echo "Copy JDBC driver in the drivers folder and reference it in the classpath below (--jars argument)"
    exit 1
fi


export SPARK_DRIVER_MEMORY=1G

export COMET_ROOT=gen
export COMET_FS=file://
export COMET_ROOT=gen
export COMET_METRICS_ACTIVE=true
export COMET_FS=file://
export COMET_ASSERTIONS_ACTIVE=true
export COMET_SINK_TO_FILE=true



COMET_JAR="comet-spark3_2.12-0.2.0.jar"
COMET_OPTIONS="--conf spark.driver.extraJavaOptions=-Dconfig.file=$PWD/application.conf"
COMET_LIBS="--jars drivers/postgresql-42.2.19.jar"
COMET_SCRIPT="spark-3.1.1-bin-hadoop2.7/bin/spark-submit $COMET_LIBS $COMET_OPTIONS --class com.ebiznext.comet.schema.generator.DDL2Yml ${COMET_JAR}"

$COMET_SCRIPT --jdbc-mapping ddl2yml-alltables.yml --output-dir output --yml-template domain-template.comet.yml
