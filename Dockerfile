# Use the Official OpenJDK image for a lean production stage of our multi-stage build.
# https://hub.docker.com/_/openjdk
# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
FROM eclipse-temurin:17

ENV SL_VERSION=0.8.0-SNAPSHOT
ENV SCALA_VERSION=2.12
ENV SPARK_VERSION="3.4.1"
ENV SPARK_SNOWFLAKE_VERSION="3.4"
ENV JDBC_SNOWFLAKE_VERSION="3.14.0"
ENV HADOOP_VERSION="3"
ENV SPARK_BQ_VERSION="0.32.0"
WORKDIR /app

# SPARK binaries
RUN curl -L -O https://downloads.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz

# BIGQUERY Dependencies
RUN curl -L -O https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.30.0/spark-bigquery-with-dependencies_2.12-$SPARK_BQ_VERSION.jar
RUN curl -L -O https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.12/gcs-connector-hadoop3-2.2.12-shaded.jar

# Azure dependency
RUN curl -L -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.5/hadoop-azure-3.3.5.jar
RUN curl -L -O https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar
RUN curl -L -O https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util/9.4.51.v20230217/jetty-util-9.4.51.v20230217.jar
RUN curl -L -O https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util-ajax/9.4.51.v20230217/jetty-util-ajax-9.4.51.v20230217.jar

# SNOWFLAKE Dependencies

RUN curl -L -O https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_$SPARK_SNOWFLAKE_VERSION/spark-snowflake_2.12-2.12.0-spark_$SPARK_SNOWFLAKE_VERSION.jar
RUN curl -L -O https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/$JDBC_SNOWFLAKE_VERSION/snowflake-jdbc-$JDBC_SNOWFLAKE_VERSION.jar

# STARLAKE Dependencies
RUN apt-get update; apt-get install -y findutils jq vim

RUN if [[ "$SL_VERSION" == *"SNAPSHOT"* ]] ;  then curl -L -O https://s01.oss.sonatype.org/content/repositories/snapshots/ai/starlake/starlake-spark3_$SCALA_VERSION/$SL_VERSION/starlake-spark3_$SCALA_VERSION-$SL_VERSION-assembly.jar ; else curl -L -O https://s01.oss.sonatype.org/content/repositories/releases/ai/starlake/starlake-spark3_$SCALA_VERSION/$SL_VERSION/starlake-spark3_$SCALA_VERSION-$SL_VERSION-assembly.jar ; fi

RUN curl -L -O https://s01.oss.sonatype.org/content/repositories/releases/ai/starlake/starlake-spark3_$SCALA_VERSION/$SL_VERSION/starlake-spark3_$SCALA_VERSION-$SL_VERSION-assembly.jar

# SPARK Configuration
RUN mkdir -p /app/bin/spark
RUN tar -zxf spark-$SPARK_VERSION-bin-hadoop3.tgz -C /app/bin/spark
RUN mv /app/bin/spark/spark-$SPARK_VERSION-bin-hadoop3/* /app/bin/spark
RUN rm spark-$SPARK_VERSION-bin-hadoop3.tgz && rm -rf /app/bin/spark/spark-$SPARK_VERSION-bin-hadoop3

# Copy Dependencies
## BigQuery
RUN mv spark-bigquery-with-dependencies_2.12-$SPARK_BQ_VERSION.jar /app/bin/spark/jars
RUN mv gcs-connector-hadoop3-2.2.12-shaded.jar /app/bin/spark/jars
## Snowflake
RUN mv spark-snowflake_2.12-2.12.0-spark_$SPARK_SNOWFLAKE_VERSION.jar /app/bin/spark/jars
RUN mv snowflake-jdbc-$JDBC_SNOWFLAKE_VERSION.jar /app/bin/spark/jars
## Azure
RUN mv hadoop-azure-3.3.5.jar /app/bin/spark/jars
RUN mv azure-storage-8.6.6.jar /app/bin/spark/jars
RUN mv jetty-util-9.4.51.v20230217.jar /app/bin/spark/jars
RUN mv jetty-util-ajax-9.4.51.v20230217.jar /app/bin/spark/jars

# Copy Starlake
RUN mv starlake-spark3_$SCALA_VERSION-$SL_VERSION-assembly.jar /app/bin/spark/jars
RUN mv /app/bin/spark/conf/log4j2.properties.template /app/bin/spark/jars/log4j2.properties
COPY distrib/starlake.sh /app/


ENTRYPOINT ["/app/starlake.sh"]
