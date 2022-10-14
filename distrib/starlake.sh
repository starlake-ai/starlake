#!/bin/bash

COMET_VERSION="${COMET_VERSION:-0.4.2}"
SPARK_VERSION="${SPARK_VERSION:-3.3.0}"
HADOOP_VERSION="${HADOOP_VERSION:-3}"
SPARK_BQ_VERSION="${SPARK_BQ_VERSION:-0.27.0-preview}"
SCALA_VERSION=2.12
SPARK_DIR_NAME=spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
COMET_JAR_NAME=starlake-spark3_$SCALA_VERSION-$COMET_VERSION-assembly.jar
COMET_JAR_FULL_NAME=bin/spark/jars/$COMET_JAR_NAME
SPARK_BQ_JAR_FULL_NAME=bin/spark/jars/spark-3.1-bigquery-$SPARK_BQ_VERSION.jar

export COMET_ENV="${COMET_ENV:-FS}"
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4G}"
export COMET_FS="${COMET_FS:-file://}"
export COMET_MAIN=ai.starlake.job.Main

STARLAKE_PATH=$( dirname -- "${BASH_SOURCE[0]}" )
TARGET_FOLDER="$(cd "$STARLAKE_PATH" && pwd)"

mkdir $TARGET_FOLDER 2>/dev/null

if [[ "$COMET_VERSION" == *"SNAPSHOT"* ]]; then
  COMET_JAR_URL=https://s01.oss.sonatype.org/content/repositories/snapshots/ai/starlake/starlake-spark3_$SCALA_VERSION/$COMET_VERSION/$COMET_JAR_NAME
else
  COMET_JAR_URL=https://s01.oss.sonatype.org/content/repositories/releases/ai/starlake/starlake-spark3_$SCALA_VERSION/$COMET_VERSION/$COMET_JAR_NAME
fi

#echo "COMET_VERSION=$COMET_VERSION"
#echo "SPARK_VERSION=$SPARK_VERSION"
#echo "HADOOP_VERSION=$HADOOP_VERSION"
#echo "COMET_JAR_URL=$COMET_JAR_URL"

SPARK_TGZ_NAME=$SPARK_DIR_NAME.tgz
SPARK_TGZ_URL=https://downloads.apache.org/spark/spark-$SPARK_VERSION/$SPARK_TGZ_NAME
SPARK_BQ_URL=https://repo1.maven.org/maven2/com/google/cloud/spark/spark-3.1-bigquery/$SPARK_BQ_VERSION/spark-3.1-bigquery-$SPARK_BQ_VERSION.jar


initEnv() {
  if [[ ! -d "$TARGET_FOLDER/bin/" ]]
  then
      mkdir $TARGET_FOLDER/bin/
  fi

  if [[ ! -d "$TARGET_FOLDER/bin/spark" ]]
  then
      mkdir $TARGET_FOLDER/bin/spark
  fi

  if ! test -f "$TARGET_FOLDER/bin/spark/bin/spark-submit"; then
    while true; do
      read -n1 -s -p "Do you wish to install starlake in $TARGET_FOLDER (Y/n)? " yn
      yn=${yn:-yes}
      case $yn in
          [Yy]* ) break;;
          [Nn]* ) echo "Installation aborted!"; exit;;
          * ) echo "Please answer yes or no.";;
      esac
    done
    echo ""
    echo "downloading $SPARK_TGZ_NAME from $SPARK_TGZ_URL"
    curl --output $TARGET_FOLDER/bin/$SPARK_TGZ_NAME $SPARK_TGZ_URL
    tar zxvf $TARGET_FOLDER/bin/$SPARK_TGZ_NAME -C $TARGET_FOLDER/bin/
    rm -f $TARGET_FOLDER/bin/$SPARK_TGZ_NAME
    mv $TARGET_FOLDER/bin/$SPARK_DIR_NAME/* $TARGET_FOLDER/bin/spark
    rm -f $TARGET_FOLDER/bin/spark/conf/*.xml 2>/dev/null
    cp $TARGET_FOLDER/bin/spark/conf/log4j2.properties.template $TARGET_FOLDER/bin/spark/conf/log4j2.properties
    echo "Spark Version: $SPARK_VERSION" >$TARGET_FOLDER/version.info
  fi

  if ! test -f "$TARGET_FOLDER/$COMET_JAR_FULL_NAME"; then
    echo "downloading $COMET_JAR_NAME from $COMET_JAR_URL"
    curl --output $TARGET_FOLDER/$COMET_JAR_FULL_NAME $COMET_JAR_URL
    echo "Starlake Version: $COMET_VERSION" >>$TARGET_FOLDER/version.info
    echo "Make sure $TARGET_FOLDER is in your path"
  fi

  if ! test -f "$TARGET_FOLDER/$SPARK_BQ_JAR_FULL_NAME"; then
    echo "downloading $SPARK_BQ_JAR_FULL_NAME from $SPARK_BQ_URL"
    curl --output $TARGET_FOLDER/$SPARK_BQ_JAR_FULL_NAME $SPARK_BQ_URL
    echo "Spark Bigquery Version Version: $SPARK_BQ_VERSION" >>$TARGET_FOLDER/version.info
  fi

  if ! test -f $SPARK_SUBMIT; then
    echo "$SPARK_SUBMIT not found !!!"
    echo "FAILURE: Failed to initialize environment"
    exit 2
  fi
  mkdir $TARGET_FOLDER/out/ 2>/dev/null
}


initEnv

#SPARK_DRIVER_OPTIONS="-Dlog4j.configuration=file://$TARGET_FOLDER/bin/spark/conf/log4j2.properties"
#export SPARK_DRIVER_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties"
SPARK_SUBMIT=$TARGET_FOLDER/bin/spark/bin/spark-submit
$SPARK_SUBMIT --driver-java-options "$SPARK_DRIVER_OPTIONS" $SPARK_CONF_OPTIONS --class $COMET_MAIN $TARGET_FOLDER/$COMET_JAR_FULL_NAME $@






