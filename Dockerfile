# Use the Official OpenJDK image for a lean production stage of our multi-stage build.
# https://hub.docker.com/_/openjdk
# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
FROM eclipse-temurin:17

ENV SL_VERSION=0.7.1
ENV SCALA_VERSION=2.12
ENV SPARK_VERSION="3.3.2"
ENV HADOOP_VERSION="3"
ENV SPARK_BQ_VERSION="0.30.0"
WORKDIR /app

# SPARK binaries
RUN curl -L -O https://downloads.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz

# BIGQUERY Dependencies
RUN curl -L -O https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.30.0/spark-bigquery-with-dependencies_2.12-0.30.0.jar
RUN curl -L -O https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.12/gcs-connector-hadoop3-2.2.12-shaded.jar

# SNOWFLAKE Dependencies
RUN curl -L -O https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.11.2-spark_3.3/spark-snowflake_2.12-2.11.2-spark_3.3.jar
RUN curl -L -O https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.30/snowflake-jdbc-3.13.30.jar

# STARLAKE Dependencies
RUN apt-get update; apt-get install -y findutils jq vim
RUN curl -L -O https://s01.oss.sonatype.org/content/repositories/releases/ai/starlake/starlake-spark3_$SCALA_VERSION/$SL_VERSION/starlake-spark3_$SCALA_VERSION-$SL_VERSION-assembly.jar

# SPARK Configuration
RUN mkdir -p /app/bin/spark
RUN tar -zxf spark-3.3.2-bin-hadoop3.tgz -C /app/bin/spark
RUN mv /app/bin/spark/spark-3.3.2-bin-hadoop3/* /app/bin/spark
RUN rm spark-3.3.2-bin-hadoop3.tgz && rm -rf /app/bin/spark/spark-3.3.2-bin-hadoop3

# Copy Dependencies
RUN mv spark-bigquery-with-dependencies_2.12-0.30.0.jar /app/bin/spark/jars
RUN mv gcs-connector-hadoop3-2.2.12-shaded.jar /app/bin/spark/jars
RUN mv spark-snowflake_2.12-2.11.2-spark_3.3.jar /app/bin/spark/jars
RUN mv snowflake-jdbc-3.13.30.jar /app/bin/spark/jars

RUN mv starlake-spark3_$SCALA_VERSION-$SL_VERSION-assembly.jar /app/bin/spark/jars
RUN mv /app/bin/spark/conf/log4j2.properties.template /app/bin/spark/jars/log4j2.properties
COPY distrib/starlake.sh /app/
ENTRYPOINT ["/app/starlake.sh"]
