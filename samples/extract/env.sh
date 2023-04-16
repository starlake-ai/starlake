source ../env.sh
export H2_PORT="${H2PORT:-9092}"
export SL_ENV=FS
export SPARK_DRIVER_MEMORY=4G
export SL_FS=file://
export SL_ROOT="$(PWD)/quickstart"
export SL_METRICS_ACTIVE=true
export SL_ASSERTIONS_ACTIVE=true
export SL_SINK_TO_FILE=true
export SL_ANALYZE=false
export SL_HIVE=false
export SL_GROUPED=false
export SL_METRICS_PATH="/tmp/metrics/{{domain}}"
export SPARK_CONF_OPTIONS="--conf spark.driver.extraJavaOptions=-Dconfig.file=$PWD/application.conf"

H2_JAR=h2-1.4.200.jar
H2_URL=https://repo1.maven.org/maven2/com/h2database/h2/1.4.200/$H2_JAR

if test -f "drivers/$H2_JAR"; then
    echo "$H2_JAR found"
else
  echo "downloading $H2_JAR from $H2_URL"
  curl --output drivers/$H2_JAR $H2_URL
fi

rm -rf output
mkdir output

#java -classpath drivers/h2*.jar org.h2.tools.Server -baseDir $PWD/h2db -pgDaemon -tcp -tcpPassword h2passwd -tcpPort $H2_PORT &

#java -classpath drivers/h2*.jar org.h2.tools.Server -tcpShutdown tcp://localhost:9092 -tcpPassword h2passwd -tcpPort $H2_PORT

# shellcheck disable=SC2006
#SQL=`cat $PWD/createdb.sql`
#java -classpath drivers/h2*.jar org.h2.tools.Shell -url jdbc:h2:file:$PWD/h2db -sql $SQL

awk -v h2db="$PWD/h2db" '{gsub("H2_DB", h2db)}1' application-template.conf >application.conf

SL_LIBS="--jars drivers/$H2_JAR"

export SL_SCRIPT="$SPARK_SUBMIT $SPARK_CONF_OPTIONS $SL_LIBS --class ai.starlake.schema.generator.JDBC2Yml $SL_JAR_FULL_NAME"
