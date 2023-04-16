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
#export SPARK_CONF_OPTIONS="--conf spark.driver.extraJavaOptions=-Dconfig.file=$PWD/application.conf"

H2_JAR=h2-1.4.200.jar
H2_URL=https://repo1.maven.org/maven2/com/h2database/h2/1.4.200/$H2_JAR

if [[ -d "../../distrib/bin/spark/jars" ]]
then
  echo "Starlake jars folder found"
else
  echo "Starlake ../../distrib/bin/spark/jars/ folder not found"
  echo "Please make sure you clones the starlake repository first and ran this script from the samples/extract folder."
  exit 1
fi

if test -f "drivers/$H2_JAR"; then
    echo "$H2_JAR found"
else
  echo "downloading $H2_JAR from $H2_URL"
  mkdir drivers
  curl --output drivers/$H2_JAR $H2_URL
fi

echo Reinitializing this sample
rm -rf output
mkdir output H2SLDB 2>/dev/null
rm $PWD/H2SLDB.mv.db $PWD/H2SLDB.trace.db 2>/dev/null


H2PID=`ps -ef | grep "[t]cpPort $H2_PORT" | awk '{print $2}'`
if [[ "$H2PID" != ""  ]]
then
  echo Stopping previous H2 server running on port $H2_PORT
  kill -9 $H2PID
fi

echo Runing H2 server on port $H2_PORT
java -classpath drivers/h2*.jar org.h2.tools.Server -pgDaemon -tcp -tcpPassword h2passwd -tcpPort $H2_PORT &

SQL=`cat $PWD/createdb.sql`
echo Loading data from file $PWD/createdb.sql
java -classpath drivers/h2*.jar org.h2.tools.Shell -url jdbc:h2:file:$PWD/H2SLDB -sql "$SQL"

awk -v h2db="$PWD/H2SLDB" '{gsub("H2_DB", h2db)}1' application-template.conf >$SL_ROOT/metadata/application.conf

#SL_LIBS="--jars drivers/$H2_JAR"
cp drivers/$H2_JAR ../../distrib/bin/spark/jars/
../../distrib/starlake.sh extract-schema --mapping ddl2yml --output-dir $SL_ROOT/metadata/domains

H2PID=`ps -ef | grep "[t]cpPort $H2_PORT" | awk '{print $2}'`

kill -9 $H2PID

rm $PWD/H2SLDB.mv.db $PWD/H2SLDB.trace.db 2>/dev/null
