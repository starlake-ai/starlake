#!/bin/bash

set -e

SCRIPT_DIR="$( cd "$( dirname -- "${BASH_SOURCE[0]}" )" && pwd )"
SL_ROOT="${SL_ROOT:-`pwd`}"
if [ -f "$SL_ROOT/sl_versions.sh" ]
then
  source "$SL_ROOT/sl_versions.sh"
fi
if [ -f "$SCRIPT_DIR/versions.sh" ]
then
  source "$SCRIPT_DIR/versions.sh"
fi

## default versions
SL_DEFAULT_VERSION="1.0.0-SNAPSHOT"
SPARK_DEFAULT_VERSION="3.5.0"
HADOOP_DEFAULT_VERSION="3"
SPARK_BQ_DEFAULT_VERSION="0.34.0"
HADOOP_AZURE_DEFAULT_VERSION="3.3.5"
AZURE_STORAGE_DEFAULT_VERSION="8.6.6"
JETTY_DEFAULT_VERSION="9.4.51.v20230217"
SPARK_SNOWFLAKE_DEFAULT_VERSION="3.4"
SNOWFLAKE_JDBC_DEFAULT_VERSION="3.14.0"
SNOWFLAKE_JDBC_DEFAULT_VERSION="3.14.0"
POSTGRESQL_DEFAULT_VERSION="42.5.4"

## Common
SL_VERSION="${SL_VERSION:-$SL_DEFAULT_VERSION}"
SPARK_VERSION="${SPARK_VERSION:-$SPARK_DEFAULT_VERSION}"
HADOOP_VERSION="${HADOOP_VERSION:-$HADOOP_DEFAULT_VERSION}"

## GCP
ENABLE_GCP=${ENABLE_GCP:-0} # skip gcp dependency install stage if 0 else install gcp missing dependencies
SPARK_BQ_VERSION="${SPARK_BQ_VERSION:-$SPARK_BQ_DEFAULT_VERSION}"

## AZURE
ENABLE_AZURE=${ENABLE_AZURE:-0} # skip azure dependency install stage if 0 else install azure missing dependencies
HADOOP_AZURE_VERSION="${HADOOP_AZURE_VERSION:-$HADOOP_AZURE_DEFAULT_VERSION}"
AZURE_STORAGE_VERSION="${AZURE_STORAGE_VERSION:-$AZURE_STORAGE_DEFAULT_VERSION}"
JETTY_VERSION="${JETTY_VERSION:-$JETTY_DEFAULT_VERSION}"
JETTY_UTIL_VERSION="${JETTY_UTIL_VERSION:-$JETTY_VERSION}"
JETTY_UTIL_AJAX_VERSION="${JETTY_UTIL_AJAX_VERSION:-$JETTY_VERSION}"

## SNOWFLAKE
ENABLE_SNOWFLAKE=${ENABLE_SNOWFLAKE:-0}
SPARK_SNOWFLAKE_VERSION="${SPARK_SNOWFLAKE_VERSION:-$SPARK_SNOWFLAKE_DEFAULT_VERSION}"
SNOWFLAKE_JDBC_VERSION="${SNOWFLAKE_JDBC_VERSION:-$SNOWFLAKE_JDBC_DEFAULT_VERSION}"

## POSTGRESQL
ENABLE_POSTGRESQL=${ENABLE_POSTGRESQL:-0}
POSTGRESQL_VERSION="${POSTGRESQL_VERSION:-$POSTGRESQL_DEFAULT_VERSION}"

# internal variables
SCALA_VERSION=2.12
SKIP_INSTALL=0
SL_ARTIFACT_NAME=starlake-spark3_$SCALA_VERSION
SPARK_DIR_NAME=spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
SPARK_TARGET_FOLDER=$SCRIPT_DIR/bin/spark
SPARK_EXTRA_LIB_FOLDER=$SCRIPT_DIR/bin
DEPS_EXTRA_LIB_FOLDER=$SPARK_EXTRA_LIB_FOLDER/deps
STARLAKE_EXTRA_LIB_FOLDER=$SPARK_EXTRA_LIB_FOLDER/sl
#SPARK_EXTRA_PACKAGES="--packages io.delta:delta-core_2.12:2.4.0"
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4G}"
export SL_MAIN=ai.starlake.job.Main
export SL_VALIDATE_ON_LOAD=false
if [ -n "$SL_VERSION" ]
then
  SL_JAR_NAME=$SL_ARTIFACT_NAME-$SL_VERSION-assembly.jar
fi

SPARK_TGZ_NAME=$SPARK_DIR_NAME.tgz
SPARK_TGZ_URL=https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/$SPARK_TGZ_NAME
SPARK_JAR_NAME=spark-core_$SCALA_VERSION-$SPARK_VERSION.jar

## GCP
SPARK_BQ_ARTIFACT_NAME=spark-bigquery-with-dependencies_$SCALA_VERSION
SPARK_BQ_JAR_NAME=$SPARK_BQ_ARTIFACT_NAME-$SPARK_BQ_VERSION.jar
SPARK_BQ_URL=https://repo1.maven.org/maven2/com/google/cloud/spark/$SPARK_BQ_ARTIFACT_NAME/$SPARK_BQ_VERSION/$SPARK_BQ_JAR_NAME

## AZURE
HADOOP_AZURE_ARTIFACT_NAME=hadoop-azure
HADOOP_AZURE_JAR_NAME=$HADOOP_AZURE_ARTIFACT_NAME-$HADOOP_AZURE_VERSION.jar
HADOOP_AZURE_URL=https://repo1.maven.org/maven2/org/apache/hadoop/$HADOOP_AZURE_ARTIFACT_NAME/$HADOOP_AZURE_VERSION/$HADOOP_AZURE_JAR_NAME
AZURE_STORAGE_ARTIFACT_NAME=azure-storage
AZURE_STORAGE_JAR_NAME=$AZURE_STORAGE_ARTIFACT_NAME-$AZURE_STORAGE_VERSION.jar
AZURE_STORAGE_URL=https://repo1.maven.org/maven2/com/microsoft/azure/$AZURE_STORAGE_ARTIFACT_NAME/$AZURE_STORAGE_VERSION/$AZURE_STORAGE_JAR_NAME
JETTY_UTIL_ARTIFACT_NAME=jetty-util
JETTY_UTIL_JAR_NAME=$JETTY_UTIL_ARTIFACT_NAME-$JETTY_UTIL_VERSION.jar
JETTY_UTIL_URL=https://repo1.maven.org/maven2/org/eclipse/jetty/$JETTY_UTIL_ARTIFACT_NAME/$JETTY_UTIL_VERSION/$JETTY_UTIL_JAR_NAME
JETTY_UTIL_AJAX_ARTIFACT_NAME=jetty-util-ajax
JETTY_UTIL_AJAX_JAR_NAME=$JETTY_UTIL_AJAX_ARTIFACT_NAME-$JETTY_UTIL_AJAX_VERSION.jar
JETTY_UTIL_AJAX_URL=https://repo1.maven.org/maven2/org/eclipse/jetty/$JETTY_UTIL_AJAX_ARTIFACT_NAME/$JETTY_UTIL_AJAX_VERSION/$JETTY_UTIL_AJAX_JAR_NAME

## SNOWFLAKE
SPARK_SNOWFLAKE_ARTIFACT_NAME=spark-snowflake_$SCALA_VERSION
SPARK_SNOWFLAKE_FULL_VERSION=$SCALA_VERSION.0-spark_$SPARK_SNOWFLAKE_VERSION
SPARK_SNOWFLAKE_JAR_NAME=$SPARK_SNOWFLAKE_ARTIFACT_NAME-$SPARK_SNOWFLAKE_FULL_VERSION.jar
SPARK_SNOWFLAKE_URL=https://repo1.maven.org/maven2/net/snowflake/$SPARK_SNOWFLAKE_ARTIFACT_NAME/$SPARK_SNOWFLAKE_FULL_VERSION/$SPARK_SNOWFLAKE_JAR_NAME
SNOWFLAKE_JDBC_ARTIFACT_NAME=snowflake-jdbc
SNOWFLAKE_JDBC_JAR_NAME=$SNOWFLAKE_JDBC_ARTIFACT_NAME-$SNOWFLAKE_JDBC_VERSION.jar
SNOWFLAKE_JDBC_URL=https://repo1.maven.org/maven2/net/snowflake/$SNOWFLAKE_JDBC_ARTIFACT_NAME/$SNOWFLAKE_JDBC_VERSION/$SNOWFLAKE_JDBC_JAR_NAME

# POSTGRESQL
POSTGRESQL_ARTIFACT_NAME=postgresql
POSTGRESQL_JAR_NAME=$POSTGRESQL_ARTIFACT_NAME-$POSTGRESQL_VERSION.jar
POSTGRESQL_URL=https://repo1.maven.org/maven2/org/postgresql/$POSTGRESQL_ARTIFACT_NAME/$POSTGRESQL_VERSION/$POSTGRESQL_JAR_NAME

init_starlake_install_variables() {
  if [[ -z "$SL_VERSION" ]]; then
    if [[ ENABLE_BIGQUERY -eq 0 && ENABLE_AZURE -eq 0 && ENABLE_SNOWFLAKE -eq 0 && ENABLE_POSTGRESQL -eq 0 ]]
    then
      echo "No dependency enabled, all dependency will be installed"
      ENABLE_BIGQUERY=1
      ENABLE_AZURE=1
      ENABLE_SNOWFLAKE=1
      ENABLE_POSTGRESQL=1
    fi

    echo "searching for last starlake version, please wait ..."
    SL_VERSION=$(curl -s --fail "https://search.maven.org/solrsearch/select?q=g:ai.starlake+AND+a:$SL_ARTIFACT_NAME&core=gav&start=0&rows=42&wt=json" | jq -r '.response.docs[].v' | sed '#-#!{s/$/_/}' | sort -Vr | sed 's/_$//' | head -n 1)
    echo got version $SL_VERSION
  echo "-------------------"
  fi
  SL_JAR_NAME=$SL_ARTIFACT_NAME-$SL_VERSION-assembly.jar
  if [[ "$SL_VERSION" == *"SNAPSHOT"* ]]
  then
    SL_JAR_URL=https://s01.oss.sonatype.org/content/repositories/snapshots/ai/starlake/starlake-spark3_$SCALA_VERSION/$SL_VERSION/$SL_JAR_NAME
  else
    SL_JAR_URL=https://s01.oss.sonatype.org/content/repositories/releases/ai/starlake/starlake-spark3_$SCALA_VERSION/$SL_VERSION/$SL_JAR_NAME
  fi

}

check_current_state() {
  echo "-------------------"
  echo "   Current state   "
  echo "-------------------"

  SPARK_DOWNLOADED=0
  SL_DOWNLOADED=0
  SPARK_BQ_DOWNLOADED=0
  HADOOP_AZURE_DOWNLOADED=0
  AZURE_STORAGE_DOWNLOADED=0
  JETTY_UTIL_DOWNLOADED=0
  JETTY_UTIL_AJAX_DOWNLOADED=0
  SPARK_SNOWFLAKE_DOWNLOADED=0
  SNOWFLAKE_JDBC_DOWNLOADED=0
  POSTGRESQL_DOWNLOADED=0

# STARLAKE
  if [[ -d "$STARLAKE_EXTRA_LIB_FOLDER" && -n "$SL_JAR_NAME" && -f "$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME" ]]
  then
    echo - starlake: found
    SL_DOWNLOADED=1
  else
    init_starlake_install_variables
    echo - starlake: not found
    SKIP_INSTALL=1
  fi

# SPARK
  if [[ -f "$SPARK_TARGET_FOLDER/jars/$SPARK_JAR_NAME" ]]
  then
    echo - spark: found
    SPARK_DOWNLOADED=1
  else
    echo - spark: not found
    SKIP_INSTALL=1
  fi
  if [[ $ENABLE_BIGQUERY -eq 1 ]]
  then
    if [[ -f "$DEPS_EXTRA_LIB_FOLDER/$SPARK_BQ_JAR_NAME" ]]
    then
      echo - spark bq: found
      SPARK_BQ_DOWNLOADED=1
    else
      echo - spark bq: not found
      SKIP_INSTALL=1
    fi
  else
    SPARK_BQ_DOWNLOADED=0
    echo - spark bq: skipped
  fi

# AZURE
  if [[ $ENABLE_AZURE -eq 1 ]]
  then
    if [[ -f "$DEPS_EXTRA_LIB_FOLDER/$HADOOP_AZURE_JAR_NAME" ]]
    then
      echo - hadoop azure: found
      HADOOP_AZURE_DOWNLOADED=1
    else
      echo - hadoop azure: not found
      SKIP_INSTALL=1
    fi
    if [[ -f "$DEPS_EXTRA_LIB_FOLDER/$AZURE_STORAGE_JAR_NAME" ]]
    then
      echo - azure storage: found
      AZURE_STORAGE_DOWNLOADED=1
    else
      echo - azure storage: not found
      SKIP_INSTALL=1
    fi
    if [[ -f "$DEPS_EXTRA_LIB_FOLDER/$JETTY_UTIL_JAR_NAME" ]]
    then
      echo - jetty util: found
      JETTY_UTIL_DOWNLOADED=1
    else
      echo - jetty util: not found
      SKIP_INSTALL=1
    fi
    if [[ -f "$DEPS_EXTRA_LIB_FOLDER/$JETTY_UTIL_AJAX_JAR_NAME" ]]
    then
      echo - jetty util ajax: found
      JETTY_UTIL_AJAX_DOWNLOADED=1
    else
      echo - jetty util ajax: not found
      SKIP_INSTALL=1
    fi
  else
    HADOOP_AZURE_DOWNLOADED=1
    AZURE_STORAGE_DOWNLOADED=1
    JETTY_UTIL_DOWNLOADED=1
    JETTY_UTIL_AJAX_DOWNLOADED=1
    echo - hadoop azure: skipped
    echo - azure storage: skipped
    echo - jetty util: skipped
    echo - jetty util ajax: skipped
  fi

# SNOWFLAKE
  if [[ $ENABLE_SNOWFLAKE -eq 1 ]]
  then
    if [[ -f "$DEPS_EXTRA_LIB_FOLDER/$SPARK_SNOWFLAKE_JAR_NAME" ]]
    then
      echo - spark snowflake: OK
      SPARK_SNOWFLAKE_DOWNLOADED=1
    else
      echo - spark snowflake: KO
      SKIP_INSTALL=1
    fi
    if [[ -f "$DEPS_EXTRA_LIB_FOLDER/$SNOWFLAKE_JDBC_JAR_NAME" ]]
    then
      echo - snowflake jdbc: OK
      SNOWFLAKE_JDBC_DOWNLOADED=1
    else
      echo - snowflake jdbc: KO
      SKIP_INSTALL=1
    fi
  else
    SPARK_SNOWFLAKE_DOWNLOADED=1
    SNOWFLAKE_JDBC_DOWNLOADED=1
    echo - spark snowflake: skipped
    echo - snowflake jdbc: skipped
  fi

# POSTGRESQL
  if [[ $ENABLE_POSTGRESQL -eq 1 ]]
  then
    if [[ -f "$DEPS_EXTRA_LIB_FOLDER/$POSTGRESQL_JAR_NAME" ]]
    then
      echo - postgresql: OK
      POSTGRESQL_DOWNLOADED=1
    else
      echo - postgresql: KO
      SKIP_INSTALL=1
    fi
  else
    POSTGRESQL_DOWNLOADED=1
    echo - postgresql: skipped
  fi
}

clean_additional_jars() {
  # prevent same jar with different version in classpath
  if [[ $SL_DOWNLOADED -eq 0 ]]
  then
    rm -f "$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME"
  fi
  if [[ $SPARK_DOWNLOADED -eq 0 ]]
  then
    rm -rf "$SPARK_TARGET_FOLDER"
  fi
  if [[ $SPARK_BQ_DOWNLOADED -eq 0 || $ENABLE_BIGQUERY -eq 0 ]]
  then
    rm -f "$DEPS_EXTRA_LIB_FOLDER/$SPARK_BQ_ARTIFACT_NAME"*
  fi
  if [[ $HADOOP_AZURE_DOWNLOADED -eq 0 || $ENABLE_AZURE -eq 0 ]]
  then
    rm -f "$DEPS_EXTRA_LIB_FOLDER/$HADOOP_AZURE_ARTIFACT_NAME"*
  fi
  if [[ $AZURE_STORAGE_DOWNLOADED -eq 0 || $ENABLE_AZURE -eq 0 ]]
  then
    rm -f "$DEPS_EXTRA_LIB_FOLDER/$AZURE_STORAGE_ARTIFACT_NAME"*
  fi
  if [[ $JETTY_UTIL_DOWNLOADED -eq 0 || $ENABLE_AZURE -eq 0 ]]
  then
    rm -f "$DEPS_EXTRA_LIB_FOLDER/$JETTY_UTIL_ARTIFACT_NAME"*
    if [[ $ENABLE_AZURE -eq 1 && $JETTY_UTIL_AJAX_DOWNLOADED -eq 1 ]]
    then
      echo "force jetty util ajax download"
      JETTY_UTIL_AJAX_DOWNLOADED=0
    fi
  fi
  if [[ $JETTY_UTIL_AJAX_DOWNLOADED -eq 0 || $ENABLE_AZURE -eq 0 ]]
  then
    rm -f "$DEPS_EXTRA_LIB_FOLDER/$JETTY_UTIL_AJAX_ARTIFACT_NAME"*
  fi
  if [[ $SPARK_SNOWFLAKE_DOWNLOADED -eq 0 || $ENABLE_SNOWFLAKE -eq 0 ]]
  then
    rm -f "$DEPS_EXTRA_LIB_FOLDER/$SPARK_SNOWFLAKE_ARTIFACT_NAME"*
  fi
  if [[ $SNOWFLAKE_JDBC_DOWNLOADED -eq 0 || $ENABLE_SNOWFLAKE -eq 0 ]]
  then
    rm -f "$DEPS_EXTRA_LIB_FOLDER/$SNOWFLAKE_JDBC_ARTIFACT_NAME"*
  fi
  if [[ $POSTGRESQL_DOWNLOADED -eq 0 || $ENABLE_POSTGRESQL -eq 0 ]]
  then
    rm -f "$DEPS_EXTRA_LIB_FOLDER/$POSTGRESQL_ARTIFACT_NAME"*
  fi
}

init_env() {
  echo
  echo "-------------------"
  echo "      Install      "
  echo "-------------------"
  mkdir -p "$SCRIPT_DIR/bin" "$STARLAKE_EXTRA_LIB_FOLDER" "$DEPS_EXTRA_LIB_FOLDER"

  if [[ $SL_DOWNLOADED -eq 0 ]]
  then
    echo "- starlake: downloading from $SL_JAR_URL"
    curl --fail --output "$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME" "$SL_JAR_URL"
    echo "- starlake: OK"
  else
    echo "- starlake: skipped"
  fi

  if [[ $SPARK_DOWNLOADED -eq 0 ]]
  then
    echo ""
    echo "- spark: downloading from $SPARK_TGZ_URL"
    curl --fail --output "$SCRIPT_DIR/bin/$SPARK_TGZ_NAME" "$SPARK_TGZ_URL"
    tar zxf "$SCRIPT_DIR/bin/$SPARK_TGZ_NAME" -C "$SCRIPT_DIR/bin/"
    rm -f "$SCRIPT_DIR/bin/$SPARK_TGZ_NAME"
    mkdir -p "$SPARK_TARGET_FOLDER"
    cp -rf "$SCRIPT_DIR/bin/$SPARK_DIR_NAME/"* "$SPARK_TARGET_FOLDER/"
    rm -rf "$SCRIPT_DIR/bin/$SPARK_DIR_NAME"
    rm -f "$SPARK_TARGET_FOLDER/conf/*.xml" 2>/dev/null
    cp "$SPARK_TARGET_FOLDER/conf/log4j2.properties.template" "$SPARK_TARGET_FOLDER/conf/log4j2.properties"
    echo "- spark: OK"
  else
    echo "- spark: skipped"
  fi

  if [[ $SPARK_BQ_DOWNLOADED -eq 0 ]]
  then
    echo "- spark bq: downloading from $SPARK_BQ_URL"
    curl --fail --output "$DEPS_EXTRA_LIB_FOLDER/$SPARK_BQ_JAR_NAME" "$SPARK_BQ_URL"
    echo "- spark bq: OK"
  else
    echo "- spark bq: skipped"
  fi

  if [[ $HADOOP_AZURE_DOWNLOADED -eq 0 ]]
  then
    echo "- hadoop azure: downloading from $HADOOP_AZURE_URL"
    curl --fail --output "$DEPS_EXTRA_LIB_FOLDER/$HADOOP_AZURE_JAR_NAME" "$HADOOP_AZURE_URL"
    echo "- hadoop azure: OK"
  else
    echo "- hadoop azure: skipped"
  fi

  if [[ $AZURE_STORAGE_DOWNLOADED -eq 0 ]]
  then
    echo "- azure storage: downloading from $AZURE_STORAGE_URL"
    curl --fail --output "$DEPS_EXTRA_LIB_FOLDER/$AZURE_STORAGE_JAR_NAME" "$AZURE_STORAGE_URL"
    echo "- azure storage: OK"
  else
    echo "- azure storage: skipped"
  fi
  if [[ $JETTY_UTIL_DOWNLOADED -eq 0 ]]
  then
    echo "- jetty util: downloading from $JETTY_UTIL_URL"
    curl --fail --output "$DEPS_EXTRA_LIB_FOLDER/$JETTY_UTIL_JAR_NAME" "$JETTY_UTIL_URL"
    echo "- jetty util: OK"
  else
    echo "- jetty util: skipped"
  fi
  if [[ $JETTY_UTIL_AJAX_DOWNLOADED -eq 0 ]]
  then
    echo "- jetty util ajax: downloading from $JETTY_UTIL_AJAX_URL"
    curl --fail --output "$DEPS_EXTRA_LIB_FOLDER/$JETTY_UTIL_AJAX_JAR_NAME" "$JETTY_UTIL_AJAX_URL"
    echo "- jetty util ajax: OK"
  else
    echo "- jetty util ajax: skipped"
  fi
  if [[ $SPARK_SNOWFLAKE_DOWNLOADED -eq 0 ]]
  then
    echo "- spark snowflake: downloading from $SPARK_SNOWFLAKE_URL"
    curl --fail --output "$DEPS_EXTRA_LIB_FOLDER/$SPARK_SNOWFLAKE_JAR_NAME" "$SPARK_SNOWFLAKE_URL"
    echo "- spark snowflake: OK"
  else
    echo "- spark snowflake: skipped"
  fi
  if [[ $SNOWFLAKE_JDBC_DOWNLOADED -eq 0 ]]
  then
    echo "- snowflake jdbc: downloading from $SNOWFLAKE_JDBC_URL"
    curl --fail --output "$DEPS_EXTRA_LIB_FOLDER/$SNOWFLAKE_JDBC_JAR_NAME" "$SNOWFLAKE_JDBC_URL"
    echo "- snowflake jdbc: OK"
  else
    echo "- snowflake jdbc: skipped"
  fi
  if [[ $POSTGRESQL_DOWNLOADED -eq 0 ]]
  then
    echo "- postgresql: downloading from $POSTGRESQL_URL"
    curl --fail --output "$DEPS_EXTRA_LIB_FOLDER/$POSTGRESQL_JAR_NAME" "$POSTGRESQL_URL"
    echo "- postgresql: OK"
  else
    echo "- postgresql: skipped"
  fi
}

print_install_usage() {
  echo "Starlake is not installed yet. Please type 'starlake.sh install'."
  echo You can define the different env vars if you need to install specific versions.
  echo
  echo SL_VERSION: Support stable and snapshot version. Default to latest stable version
  echo SPARK_VERSION: default $SPARK_DEFAULT_VERSION
  echo HADOOP_VERSION: default $HADOOP_DEFAULT_VERSION

  # GCP
  echo
  echo 'ENABLE_BIGQUERY: enable or disable gcp dependencies (1 or 0). Default 0 - disabled'
  echo - SPARK_BQ_VERSION: default $SPARK_BQ_DEFAULT_VERSION

  # AZURE
  echo
  echo 'ENABLE_AZURE: enable or disable azure dependencies (1 or 0). Default 0 - disabled'
  echo - HADOOP_AZURE_VERSION: default $HADOOP_AZURE_DEFAULT_VERSION
  echo - AZURE_STORAGE_VERSION: default $AZURE_STORAGE_DEFAULT_VERSION
  echo - JETTY_VERSION: default $JETTY_DEFAULT_VERSION
  echo - JETTY_UTIL_VERSION: default to JETTY_VERSION
  echo - JETTY_UTIL_AJAX_VERSION: default to JETTY_VERSION

  # SNOWFLAKE
  echo
  echo 'ENABLE_SNOWFLAKE: enable or disable snowflake dependencies (1 or 0). Default 0 - disabled'
  echo - SPARK_SNOWFLAKE_VERSION: default $SPARK_SNOWFLAKE_DEFAULT_VERSION
  echo - SNOWFLAKE_JDBC_VERSION: default $SNOWFLAKE_JDBC_DEFAULT_VERSION
  echo
  echo Example:
  echo
  echo   ENABLE_SNOWFLAKE=1 starlake.sh install
  echo
  echo "Once installed, 'versions.sh' will be generated and pin dependencies' version."
  echo

  # POSTGRESQL
  echo
  echo 'ENABLE_POSTGRESQL: enable or disable snowflake dependencies (1 or 0). Default 0 - disabled'
  echo - POSTGRESQL_VERSION: default $POSTGRESQL_DEFAULT_VERSION
  echo
  echo Example:
  echo
  echo   ENABLE_POSTGRESQL=1 starlake.sh install
  echo
  echo "Once installed, 'versions.sh' will be generated and pin dependencies' version."
  echo
}

save_installed_versions(){
  echo "#!/bin/bash" > "${SCRIPT_DIR}/versions.sh"
  echo "set -e" >> "${SCRIPT_DIR}/versions.sh"
  echo SL_VERSION="\${SL_VERSION:-${SL_VERSION}}" >> "${SCRIPT_DIR}/versions.sh"
  echo SPARK_VERSION="\${SPARK_VERSION:-${SPARK_VERSION}}" >> "${SCRIPT_DIR}/versions.sh"
  echo HADOOP_VERSION="\${HADOOP_VERSION:-${HADOOP_VERSION}}" >> "${SCRIPT_DIR}/versions.sh"
  echo ENABLE_BIGQUERY="\${ENABLE_BIGQUERY:-${ENABLE_BIGQUERY}}" >> "${SCRIPT_DIR}/versions.sh"
  if [[ $ENABLE_BIGQUERY -eq 1 ]]
  then
      echo SPARK_BQ_VERSION="\${SPARK_BQ_VERSION:-${SPARK_BQ_VERSION}}" >> "${SCRIPT_DIR}/versions.sh"
  fi
  echo ENABLE_AZURE="\${ENABLE_AZURE:-${ENABLE_AZURE}}" >> "${SCRIPT_DIR}/versions.sh"
  if [[ ENABLE_AZURE -eq 1 ]]
  then
      echo HADOOP_AZURE_VERSION="\${HADOOP_AZURE_VERSION:-${HADOOP_AZURE_VERSION}}" >> "${SCRIPT_DIR}/versions.sh"
      echo AZURE_STORAGE_VERSION="\${AZURE_STORAGE_VERSION:-${AZURE_STORAGE_VERSION}}" >> "${SCRIPT_DIR}/versions.sh"
      echo 'if [[ -z "$JETTY_VERSION" ]]' >> "${SCRIPT_DIR}/versions.sh"
      echo 'then' >> "${SCRIPT_DIR}/versions.sh"
      echo "  JETTY_UTIL_VERSION=\"\${JETTY_UTIL_VERSION:-${JETTY_UTIL_VERSION}}\"" >> "${SCRIPT_DIR}/versions.sh"
      echo "  JETTY_UTIL_AJAX_VERSION=\"\${JETTY_UTIL_AJAX_VERSION:-${JETTY_UTIL_AJAX_VERSION}}\"" >> "${SCRIPT_DIR}/versions.sh"
      echo 'fi' >> "${SCRIPT_DIR}/versions.sh"
  fi
  echo ENABLE_SNOWFLAKE="\${ENABLE_SNOWFLAKE:-${ENABLE_SNOWFLAKE}}" >> "${SCRIPT_DIR}/versions.sh"
  if [[ ENABLE_SNOWFLAKE -eq 1 ]]
  then
      echo SPARK_SNOWFLAKE_VERSION="\${SPARK_SNOWFLAKE_VERSION:-${SPARK_SNOWFLAKE_VERSION}}" >> "${SCRIPT_DIR}/versions.sh"
      echo SNOWFLAKE_JDBC_VERSION="\${SNOWFLAKE_JDBC_VERSION:-${SNOWFLAKE_JDBC_VERSION}}" >> "${SCRIPT_DIR}/versions.sh"
  fi
  if [[ ENABLE_POSTGRESQL -eq 1 ]]
  then
      echo POSTGRESQL_VERSION="\${POSTGRESQL_VERSION:-${POSTGRESQL_VERSION}}" >> "${SCRIPT_DIR}/versions.sh"
  fi
}

save_contextual_versions(){
  if [[ "$SL_ROOT" != "$SCRIPT_DIR" && -n "$SL_VERSION" ]]
  then
    # save shell version
    echo "#!/bin/bash" > "$SL_ROOT/sl_versions.sh"
    echo "set -e" >> "$SL_ROOT/sl_versions.sh"
    echo SL_VERSION="${SL_VERSION}" >> "$SL_ROOT/sl_versions.sh"

    # save batch version
    echo "@echo off" > "$SL_ROOT/sl_versions.cmd"
    echo "\"set SL_VERSION=${SL_VERSION}\"" >> "$SL_ROOT/sl_versions.cmd"
  fi
}

launch_starlake() {
  if [ -f "$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME" ]
  then
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

    if [[ -z "$SL_DEBUG" ]]
    then
      SPARK_DRIVER_OPTIONS="-Dlog4j.configuration=file://$SCRIPT_DIR/bin/spark/conf/log4j2.properties"
    else
      SPARK_DRIVER_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -Dlog4j.configuration=file://$SPARK_DIR/conf/log4j2.properties"
    fi

    if [[ "$SL_DEFAULT_LOADER" == "native" ]]
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
      if [ $(ls "$DEPS_EXTRA_LIB_FOLDER/"*.jar | wc -l) -ne 0 ]
      then
        extra_classpath="$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME:$(echo "$DEPS_EXTRA_LIB_FOLDER/"*.jar | tr ' ' ':')"
        extra_jars="$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME,$(echo "$DEPS_EXTRA_LIB_FOLDER/"*.jar | tr ' ' ',')"
      fi
      SPARK_SUBMIT="$SPARK_TARGET_FOLDER/bin/spark-submit"
      SL_ROOT="$SL_ROOT" "$SPARK_SUBMIT" $SPARK_EXTRA_PACKAGES --driver-java-options "$SPARK_DRIVER_OPTIONS" $SPARK_CONF_OPTIONS --driver-class-path "$extra_classpath" --class "$SL_MAIN" --jars "$extra_jars" "$STARLAKE_EXTRA_LIB_FOLDER/$SL_JAR_NAME" "$@"
    fi
  else
    print_install_usage
  fi
}

case "$1" in
  install)
    check_current_state
    clean_additional_jars
    if [[ $SKIP_INSTALL -eq 1 ]]
    then
      init_env
    fi
    save_installed_versions
    save_contextual_versions
    echo
    echo "Installation done. You're ready to enjoy Starlake!"
    echo If any errors happen during installation. Please try to install again or open an issue.
    ;;
  *)
    save_contextual_versions
    launch_starlake "$@"
    ;;
esac
