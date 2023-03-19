#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname -- "${BASH_SOURCE[0]}" )" && pwd )"
COMET_ROOT="${COMET_ROOT:-`pwd`}"
SPARK_VERSION="${SPARK_VERSION:-3.3.1}"
HADOOP_VERSION="${HADOOP_VERSION:-3}"
SPARK_BQ_VERSION="${SPARK_BQ_VERSION:-0.27.1}"
SCALA_VERSION=2.12
STARLAKE_ARTIFACT_NAME=starlake-spark3_$SCALA_VERSION
SPARK_DIR_NAME=spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
SPARK_TARGET_FOLDER=$SCRIPT_DIR/bin/spark
SPARK_BQ_ARTIFACT_NAME=spark-bigquery-with-dependencies_$SCALA_VERSION
SPARK_BQ_JAR_NAME=$SPARK_BQ_ARTIFACT_NAME-$SPARK_BQ_VERSION.jar

SKIP_INSTALL=0
export COMET_ENV="${COMET_ENV:-FS}"
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4G}"
export COMET_FS="${COMET_FS:-file://}"
export COMET_MAIN=ai.starlake.job.Main
export COMET_VALIDATE_ON_LOAD=false

SPARK_TGZ_NAME=$SPARK_DIR_NAME.tgz
SPARK_TGZ_URL=https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/$SPARK_TGZ_NAME
SPARK_BQ_URL=https://repo1.maven.org/maven2/com/google/cloud/spark/$SPARK_BQ_ARTIFACT_NAME/$SPARK_BQ_VERSION/$SPARK_BQ_ARTIFACT_NAME-$SPARK_BQ_VERSION.jar

initStarlakeInstallVariables() {
  if [[ -z "$COMET_VERSION" ]]; then
      COMET_VERSION=$(curl -s --fail "https://search.maven.org/solrsearch/select?q=g:ai.starlake+AND+a:$STARLAKE_ARTIFACT_NAME&core=gav&start=0&rows=42&wt=json" | jq -r '.response.docs[].v' | sed '#-#!{s/$/_/}' | sort -Vr | sed 's/_$//' | head -n 1)
  fi
  STARLAKE_JAR_NAME=$STARLAKE_ARTIFACT_NAME-$COMET_VERSION-assembly.jar
  if [[ "$COMET_VERSION" == *"SNAPSHOT"* ]]
  then
    STARLAKE_JAR_URL=https://s01.oss.sonatype.org/content/repositories/snapshots/ai/starlake/starlake-spark3_$SCALA_VERSION/$COMET_VERSION/$STARLAKE_JAR_NAME
  else
    STARLAKE_JAR_URL=https://s01.oss.sonatype.org/content/repositories/releases/ai/starlake/starlake-spark3_$SCALA_VERSION/$COMET_VERSION/$STARLAKE_JAR_NAME
  fi
}

checkCurrentState() {
  echo "-------------------"
  echo "   Current state   "
  echo "-------------------"

  SPARK_DOWNLOADED=1
  STARLAKE_DOWNLOADED=1
  SPARK_BQ_DOWNLOADED=1

  if [[ -d "$SPARK_TARGET_FOLDER/jars" ]]
  then
    echo - spark: OK
    SPARK_DOWNLOADED=0
    STARLAKE_JAR_RELATIVE_PATH=$(find "$SPARK_TARGET_FOLDER/jars" -maxdepth 1 -type f -name "$STARLAKE_ARTIFACT_NAME*")
    if [[ -z "$STARLAKE_JAR_RELATIVE_PATH" ]]
    then
      initStarlakeInstallVariables
      echo - starlake: KO
      SKIP_INSTALL=1
    else
      STARLAKE_JAR_NAME=$(basename "$STARLAKE_JAR_RELATIVE_PATH")
      echo - starlake: OK
      STARLAKE_DOWNLOADED=0
    fi
    if [[ $(find "$SPARK_TARGET_FOLDER/jars" -maxdepth 1 -type f -name "$SPARK_BQ_ARTIFACT_NAME*" | wc -l) -eq 1 ]]
    then
      echo - spark bq: OK
      SPARK_BQ_DOWNLOADED=0
    else
      echo - spark bq: KO
      SKIP_INSTALL=1
    fi

  else
    initStarlakeInstallVariables
    SKIP_INSTALL=1
    echo - spark: KO
    echo - starlake: KO
    echo - spark bq: KO
  fi
}

initEnv() {
  echo
  echo "-------------------"
  echo "      Install      "
  echo "-------------------"
  if [[ $SPARK_DOWNLOADED -eq 1 ]]
  then
    mkdir -p "$SCRIPT_DIR/bin"
    echo ""
    echo "- spark: downloading from $SPARK_TGZ_URL"
    curl --fail --output "$SCRIPT_DIR/bin/$SPARK_TGZ_NAME" "$SPARK_TGZ_URL"
    tar zxf "$SCRIPT_DIR/bin/$SPARK_TGZ_NAME" -C "$SCRIPT_DIR/bin/"
    rm -f "$SCRIPT_DIR/bin/$SPARK_TGZ_NAME"
    mv "$SCRIPT_DIR/bin/$SPARK_DIR_NAME" "$SPARK_TARGET_FOLDER"
    rm -f "$SPARK_TARGET_FOLDER/conf/*.xml" 2>/dev/null
    cp "$SPARK_TARGET_FOLDER/conf/log4j2.properties.template" "$SPARK_TARGET_FOLDER/conf/log4j2.properties"
    echo "Spark Version: $SPARK_VERSION" >> "$SCRIPT_DIR/version.info"
    echo "- spark: OK"
  else
    echo "- spark: skipped"
  fi

  if [[ STARLAKE_DOWNLOADED -eq 1 ]]
  then
    echo "- starlake: downloading from $STARLAKE_JAR_URL"
    curl --fail --output "$SPARK_TARGET_FOLDER/jars/$STARLAKE_JAR_NAME" "$STARLAKE_JAR_URL"
    echo "Starlake Version: $COMET_VERSION" >> "$SCRIPT_DIR/version.info"
    echo "- starlake: OK"
  else
    echo "- starlake: skipped"
  fi

  if [[ SPARK_BQ_DOWNLOADED -eq 1 ]]
  then
    echo "- spark bq: downloading from $SPARK_BQ_URL"
    curl --fail --output "$SPARK_TARGET_FOLDER/jars/$SPARK_BQ_JAR_NAME" "$SPARK_BQ_URL"
    echo "Spark Bigquery Version Version: $SPARK_BQ_VERSION" >> "$SCRIPT_DIR/version.info"
    echo "- spark bq: OK"
  else
    echo "- spark bq: skipped"
  fi
}

checkCurrentState
if [[ $SKIP_INSTALL -eq 1 ]]
then
  initEnv
fi

echo
echo Launching starlake.
echo "- JAVA_HOME=$JAVA_HOME"
echo "- COMET_ROOT=$COMET_ROOT"
echo "- COMET_ENV=$COMET_ENV"
echo "- COMET_FS=$COMET_FS"
echo "- COMET_MAIN=$COMET_MAIN"
echo "- COMET_VALIDATE_ON_LOAD=$COMET_VALIDATE_ON_LOAD"
echo "- SPARK_DRIVER_MEMORY=$SPARK_DRIVER_MEMORY"
echo Make sure your java home path does not contain space

#SPARK_DRIVER_OPTIONS="-Dlog4j.configuration=file://$SCRIPT_DIR/bin/spark/conf/log4j2.properties"
#export SPARK_DRIVER_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Dlog4j.configuration=file://$SPARK_DIR/conf/log4j.properties"
SPARK_SUBMIT="$SPARK_TARGET_FOLDER/bin/spark-submit"
COMET_ROOT=$COMET_ROOT $SPARK_SUBMIT --driver-java-options "$SPARK_DRIVER_OPTIONS" $SPARK_CONF_OPTIONS --class $COMET_MAIN $SPARK_TARGET_FOLDER/jars/$STARLAKE_JAR_NAME $@
