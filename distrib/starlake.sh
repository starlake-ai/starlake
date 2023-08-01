#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname -- "${BASH_SOURCE[0]}" )" && pwd )"
SL_ROOT="${SL_ROOT:-`pwd`}"
SCALA_VERSION=2.12
SPARK_VERSION="${SPARK_VERSION:-3.4.1}"
HADOOP_VERSION="${HADOOP_VERSION:-3}"
SPARK_BQ_VERSION="${SPARK_BQ_VERSION:-0.32.0}"
SL_ARTIFACT_NAME=starlake-spark3_$SCALA_VERSION
SPARK_DIR_NAME=spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
SPARK_TARGET_FOLDER=$SCRIPT_DIR/bin/spark
SPARK_BQ_ARTIFACT_NAME=spark-bigquery-with-dependencies_$SCALA_VERSION
SPARK_BQ_JAR_NAME=$SPARK_BQ_ARTIFACT_NAME-$SPARK_BQ_VERSION.jar

SKIP_INSTALL=1
export SL_ENV="${SL_ENV:-FS}"
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4G}"
export SL_MAIN=ai.starlake.job.Main
export SL_VALIDATE_ON_LOAD=false

SPARK_TGZ_NAME=$SPARK_DIR_NAME.tgz
SPARK_TGZ_URL=https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/$SPARK_TGZ_NAME
SPARK_BQ_URL=https://repo1.maven.org/maven2/com/google/cloud/spark/$SPARK_BQ_ARTIFACT_NAME/$SPARK_BQ_VERSION/$SPARK_BQ_ARTIFACT_NAME-$SPARK_BQ_VERSION.jar

initStarlakeInstallVariables() {
  if [[ -z "$SL_VERSION" ]]; then
      SL_VERSION=$(curl -s --fail "https://search.maven.org/solrsearch/select?q=g:ai.starlake+AND+a:$SL_ARTIFACT_NAME&core=gav&start=0&rows=42&wt=json" | jq -r '.response.docs[].v' | sed '#-#!{s/$/_/}' | sort -Vr | sed 's/_$//' | head -n 1)
  fi
  SL_JAR_NAME=$SL_ARTIFACT_NAME-$SL_VERSION-assembly.jar
  if [[ "$SL_VERSION" == *"SNAPSHOT"* ]]
  then
    SL_JAR_URL=https://s01.oss.sonatype.org/content/repositories/snapshots/ai/starlake/starlake-spark3_$SCALA_VERSION/$SL_VERSION/$SL_JAR_NAME
  else
    SL_JAR_URL=https://s01.oss.sonatype.org/content/repositories/releases/ai/starlake/starlake-spark3_$SCALA_VERSION/$SL_VERSION/$SL_JAR_NAME
  fi
}

checkCurrentState() {
  echo "-------------------"
  echo "   Current state   "
  echo "-------------------"

  SPARK_DOWNLOADED=1
  SL_DOWNLOADED=1
  SPARK_BQ_DOWNLOADED=1

  if [[ -d "$SPARK_TARGET_FOLDER/jars" ]]
  then
    echo - spark: OK
    SPARK_DOWNLOADED=0
    SL_JAR_RELATIVE_PATH=$(find "$SPARK_TARGET_FOLDER/jars" -maxdepth 1 -type f -name "$SL_ARTIFACT_NAME*")
    if [[ -z "$SL_JAR_RELATIVE_PATH" ]]
    then
      initStarlakeInstallVariables
      echo - starlake: KO
      SKIP_INSTALL=1
    else
      SL_JAR_NAME=$(basename "$SL_JAR_RELATIVE_PATH")
      echo - starlake: OK
      SL_DOWNLOADED=0
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

  if [[ SL_DOWNLOADED -eq 1 ]]
  then
    echo "- starlake: downloading from $SL_JAR_URL"
    curl --fail --output "$SPARK_TARGET_FOLDER/jars/$SL_JAR_NAME" "$SL_JAR_URL"
    echo "Starlake Version: $SL_VERSION" >> "$SCRIPT_DIR/version.info"
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
echo "- SL_ROOT=$SL_ROOT"
echo "- SL_ENV=$SL_ENV"
echo "- SL_MAIN=$SL_MAIN"
echo "- SL_VALIDATE_ON_LOAD=$SL_VALIDATE_ON_LOAD"
echo "- SPARK_DRIVER_MEMORY=$SPARK_DRIVER_MEMORY"
echo Make sure your java home path does not contain space


#if [[ $SL_FS = abfs:* ]] || [[ $SL_FS = wasb:* ]] || [[ $SL_FS = wasbs:* ]]
#then
#  if [[ -z "$AZURE_STORAGE_ACCOUNT" ]]
#  then
#    echo "AZURE_STORAGE_ACCOUNT should reference storage account name"
#    exit 1
#  fi
#  if [[ -z "$AZURE_STORAGE_KEY" ]]
#  then
#    echo "AZURE_STORAGE_KEY should reference the storage account key"
#    exit 1
#  fi
#  export SL_STORAGE_CONF="fs.azure.account.auth.type.$AZURE_STORAGE_ACCOUNT.blob.core.windows.net=SharedKey,
#                  fs.azure.account.key.$AZURE_STORAGE_ACCOUNT.blob.core.windows.net="$AZURE_STORAGE_KEY",
#                  fs.default.name=$SL_FS,
#                  fs.defaultFS=$SL_FS"
#fi

SPARK_DRIVER_OPTIONS="-Dlog4j.configuration=file://$SCRIPT_DIR/bin/spark/conf/log4j2.properties"
export SPARK_DRIVER_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Dlog4j.configuration=file://$SPARK_DIR/conf/log4j2.properties"
if [[ "$SL_DEFAULT_VALIDATOR" == "native" ]]
then
  SL_ROOT=$SL_ROOT java \
                      --add-opens=java.base/java.lang=ALL-UNNAMED \
                      --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
                      --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
                      --add-opens=java.base/java.io=ALL-UNNAMED \
                      --add-opens=java.base/java.net=ALL-UNNAMED \
                      --add-opens=java.base/java.nio=ALL-UNNAMED \
                      --add-opens=java.base/java.util=ALL-UNNAMED \
                      --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
                      --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
                      --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
                      --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
                      --add-opens=java.base/sun.security.action=ALL-UNNAMED \
                      --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
                      --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
                      -Dlog4j.configuration=file://$SPARK_TARGET_FOLDER/conf/log4j2.properties \
                      -cp "$SPARK_TARGET_FOLDER/jars/*" $SL_MAIN $@
else
  SPARK_SUBMIT="$SPARK_TARGET_FOLDER/bin/spark-submit"
  SL_ROOT=$SL_ROOT $SPARK_SUBMIT --driver-java-options "$SPARK_DRIVER_OPTIONS" $SPARK_CONF_OPTIONS --class $SL_MAIN $SPARK_TARGET_FOLDER/jars/$SL_JAR_NAME "$@"
fi


