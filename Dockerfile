FROM eclipse-temurin:21 as build

ARG SL_VERSION
ARG SPARK_VERSION
ARG HADOOP_VERSION

## GCP
ARG DOWNLOAD_GCP_DEPS
ARG SPARK_BQ_VERSION

## AZURE
ARG DOWNLOAD_AZURE_DEPS
ARG HADOOP_AZURE_VERSION
ARG AZURE_STORAGE_VERSION
ARG JETTY_VERSION
ARG JETTY_UTIL_VERSION
ARG JETTY_UTIL_AJAX_VERSION

## SNOWFLAKE
ARG DOWNLOAD_SNOWFLAKE_DEPS
ARG SPARK_SNOWFLAKE_VERSION
ARG SNOWFLAKE_JDBC_VERSION

WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      findutils \
      jq \
      graphviz \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY starlake.s[h] /app/
COPY versions.s[h] /app/
RUN if [ ! -f versions.sh ]; then curl -O https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/versions.sh; fi
RUN if [ ! -f starlake.sh ]; then curl -O https://raw.githubusercontent.com/starlake-ai/starlake/master/distrib/starlake.sh; fi
RUN  chmod +x starlake.sh &&  chmod +x versions.sh
RUN  SL_VERSION="$SL_VERSION" \
       SPARK_VERSION="$SPARK_VERSION" \
       HADOOP_VERSION="$HADOOP_VERSION" \
       DOWNLOAD_GCP_DEPS=$DOWNLOAD_GCP_DEPS \
       SPARK_BQ_VERSION="$SPARK_BQ_VERSION" \
       DOWNLOAD_AZURE_DEPS=$DOWNLOAD_AZURE_DEPS \
       HADOOP_AZURE_VERSION="$HADOOP_AZURE_VERSION" \
       AZURE_STORAGE_VERSION="$AZURE_STORAGE_VERSION" \
       JETTY_VERSION="$JETTY_VERSION" \
       JETTY_UTIL_VERSION="$JETTY_UTIL_VERSION" \
       JETTY_UTIL_AJAX_VERSION="$JETTY_UTIL_AJAX_VERSION" \
       DOWNLOAD_SNOWFLAKE_DEPS=$DOWNLOAD_SNOWFLAKE_DEPS \
       SPARK_SNOWFLAKE_VERSION="$SPARK_SNOWFLAKE_VERSION" \
       SNOWFLAKE_JDBC_VERSION="$SNOWFLAKE_JDBC_VERSION" \
       ./starlake.sh install



FROM eclipse-temurin:21-jre-alpine
COPY --from=build /app /app
RUN apk add --no-cache procps
ENTRYPOINT ["/app/starlake.sh"]
