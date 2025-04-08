#!/bin/bash

set -e

SCRIPT_DIR="$( cd "$( dirname -- "${BASH_SOURCE[0]}" )" && pwd )"
SL_ROOT="${SL_ROOT:-`pwd`}"
if [ -f "$SCRIPT_DIR/versions.sh" ]
then
  source "$SCRIPT_DIR/versions.sh"
fi

SL_ARTIFACT_NAME=starlake-core_$SCALA_VERSION
SPARK_DIR_NAME=spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
SPARK_TARGET_FOLDER=$SCRIPT_DIR/bin/spark
SPARK_EXTRA_LIB_FOLDER=$SCRIPT_DIR/bin
DEPS_EXTRA_LIB_FOLDER=$SPARK_EXTRA_LIB_FOLDER/deps
STARLAKE_EXTRA_LIB_FOLDER=$SPARK_EXTRA_LIB_FOLDER/sl
SL_SQL_WH="${SL_DATASETS:-$SL_ROOT/datasets}"

#SPARK_EXTRA_PACKAGES="--packages io.delta:delta-core_2.12:2.4.0"
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4g}"
export SL_MAIN=ai.starlake.job.Main
export SPARK_MASTER_URL="${SPARK_MASTER_URL:-local[*]}"

#export SL_VALIDATE_ON_LOAD=false

if [ -n "$SL_VERSION" ]
then
  SL_JAR_NAME=$SL_ARTIFACT_NAME-$SL_VERSION-assembly.jar
fi

if [[ -n "${https_proxy}" ]] || [[ -n "${http_proxy}" ]]; then
  PROXY=${https_proxy:-$http_proxy}
fi


get_binary_from_url() {
    local url=$1
    local target_file=$2
    if [ -n "$PROXY" ] && [ -n "$SL_INSECURE" ]; then
        echo "Downloading $url to $target_file using proxy $PROXY"
        local response=$(curl --insecure --proxy "$PROXY" -s -w "%{http_code}" -o "$target_file" "$url")
    else
        local response=$(curl -s -w "%{http_code}" -o "$target_file" "$url")
    fi
    local status_code=${response: -3}

    if [[ ! $status_code =~ ^(2|3)[0-9][0-9]$ ]]; then
        echo "Error: Failed to retrieve data from $url. HTTP status code: $status_code"
        exit 1
    fi
}

launch_setup() {
  local setup_url=https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/setup.jar
  get_binary_from_url $setup_url "$SCRIPT_DIR/setup.jar"

  if [ -n "${JAVA_HOME}" ]; then
    RUNNER="${JAVA_HOME}/bin/java"
  else
    if [ "$(command -v java)" ]; then
      RUNNER="java"
    else
      echo "JAVA_HOME is not set" >&2
      exit 1
    fi
  fi
  $RUNNER -cp "$SCRIPT_DIR/setup.jar" Setup "$SCRIPT_DIR" unix
}

launch_starlake() {
  if [ -f "$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME" ]
  then
    if  [ -n "$SL_LOG_LEVEL" ] && [ "$SL_LOG_LEVEL" != "error" ]; then
      echo "- JAVA_HOME=$JAVA_HOME"
      echo "- SL_ROOT=$SL_ROOT"
    fi
    if [ "$SL_ENV" != "" ]; then
      echo "- SL_ENV=$SL_ENV"
    fi
#    echo "- SL_MAIN=$SL_MAIN"
#    echo "- SL_VALIDATE_ON_LOAD=$SL_VALIDATE_ON_LOAD"
#    echo "- SPARK_DRIVER_MEMORY=$SPARK_DRIVER_MEMORY"
#    echo Make sure your java home path does not contain space


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

    if [[ -z "$SL_DEBUG" ]]
    then
      SPARK_DRIVER_OPTIONS="" # "-Dlog4j.configuration=$SPARK_TARGET_FOLDER/conf/log4j2.properties"
    else
      SPARK_DRIVER_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005" # -Dlog4j.configuration=$SPARK_TARGET_FOLDER/conf/log4j2.properties"
    fi

    if [[ "$1" =~ ^(import|xls2yml|yml2xls)$ ]]
    then
      SL_RUN_MODE=main
    fi

    if [[ "$SL_RUN_MODE" == "main" ]]
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
                          -Dlog4j.configurationFile="$SPARK_TARGET_FOLDER/conf/log4j2.properties" \
                          -cp "$SPARK_TARGET_FOLDER/jars/*:$DEPS_EXTRA_LIB_FOLDER/*:$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME" $SL_MAIN $@
    else
      extra_classpath="$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME"
      extra_jars="$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME"
      SPARK_SUBMIT="$SPARK_TARGET_FOLDER/bin/spark-submit"
      # the command below requires --jars "$extra_jars" to run on distributed modes
      if [[ $SPARK_MASTER_URL == local* ]]
      then
        if [ $(ls "$DEPS_EXTRA_LIB_FOLDER/"*.jar | wc -l) -ne 0 ]
        then
          extra_classpath="$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME:$(echo "$DEPS_EXTRA_LIB_FOLDER/"*.jar | tr ' ' ':')"
        fi
        SPARK_LOCAL_HOSTNAME="127.0.0.1" SPARK_HOME="$SCRIPT_DIR/bin/spark" SL_ROOT="$SL_ROOT" "$SPARK_SUBMIT" $SPARK_EXTRA_PACKAGES --driver-java-options "$SPARK_DRIVER_OPTIONS" $SPARK_CONF_OPTIONS --driver-class-path "$extra_classpath" --class "$SL_MAIN" --master "$SPARK_MASTER_URL" "$SPARK_TARGET_FOLDER/README.md" "$@"
      else
        if [ $(ls "$DEPS_EXTRA_LIB_FOLDER/"*.jar | wc -l) -ne 0 ]
        then
          extra_classpath="$(echo "$DEPS_EXTRA_LIB_FOLDER/"*.jar | tr ' ' ':')"
          extra_jars="$(echo "$DEPS_EXTRA_LIB_FOLDER/"*.jar | tr ' ' ',')"

        fi
         SPARK_HOME="$SCRIPT_DIR/bin/spark" SL_ROOT="$SL_ROOT" "$SPARK_SUBMIT" $SPARK_EXTRA_PACKAGES $SPARK_CONF_OPTIONS --driver-java-options "$SPARK_DRIVER_OPTIONS" --driver-class-path "$extra_classpath" --class "$SL_MAIN" --master "$SPARK_MASTER_URL"  --jars $extra_jars "$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME" "$@"
      fi
    fi
  else
    echo "Starlake jar $SL_JAR_NAME does not exists. Please install it."
    exit 1
  fi
}


case "$1" in
  --version)
	  echo Starlake $SL_VERSION
	  echo Duckdb JDBC driver ${DUCKDB_VERSION}
	  echo BigQuery Spark connector ${SPARK_BQ_VERSION}
	  echo Hadoop for Azure ${HADOOP_AZURE_VERSION}
	  echo Azure Storage ${AZURE_STORAGE_VERSION}
	  echo Spark ${SPARK_VERSION}
	  echo Hadoop ${HADOOP_VERSION}
	  echo Snowflake Spark connector ${SPARK_SNOWFLAKE_VERSION}
	  echo Snowflake JDBC driver ${SNOWFLAKE_JDBC_VERSION}
	  echo Postgres JDBC driver ${POSTGRESQL_VERSION}
	  echo AWS SDK ${AWS_JAVA_SDK_VERSION}
	  echo Hadoop for AWS ${HADOOP_AWS_VERSION}
	  echo Redshift JDBC driver ${REDSHIFT_JDBC_VERSION}
	  echo Redshift Spark connector ${SPARK_REDSHIFT_VERSION}
    ;;
  install)
    launch_setup
    echo
    echo "Installation done. You're ready to enjoy Starlake!"
    echo If any errors happen during installation. Please try to install again or open an issue.
    ;;
  serve)
    launch_starlake "$@"
    ;;
  *)
    if [[ -z "$SL_HTTP_PORT" ]]
    then
      launch_starlake "$@"
    else
      SL_HTTP_HOST=${SL_HTTP_HOST:-127.0.0.1}
      SL_SERVE_URI=http://$SL_HTTP_HOST:$SL_HTTP_PORT
      for value in validation run transform compile
      do
        log=$SL_ROOT/out/$value.log
        if [[ -f $log ]]
        then
          rm -f $log
        fi
      done
      curl  "$SL_SERVE_URI?ROOT=$SL_ROOT&PARAMS=$@"
      for value in validation run transform compile
      do
        log=$SL_ROOT/out/$value.log
        if [[ -f $log ]]
        then
          cat  $log
        fi
      done

    fi
    ;;
esac


