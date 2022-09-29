# Use the Official OpenJDK image for a lean production stage of our multi-stage build.
# https://hub.docker.com/_/openjdk
# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
# Pull base image.
FROM ubuntu

ARG STARLAKE_VERSION=0.3.26
ARG SPARK_VERSION=3.3.0
ARG HADOOP_VERSION=3

# Install Java.
RUN apt-get update && apt-get -y install openjdk-8-jdk curl
#RUN update-alternatives --set java /usr/lib/jvm/jdk1.8.0_version/bin/java


WORKDIR /app
## configure spark
RUN curl -L -O https://downloads.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz
RUN tar -zxf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -C /app
RUN rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

## download starlake
RUN curl -L -O https://s01.oss.sonatype.org/content/repositories/releases/ai/starlake/starlake-spark3_2.12/$STARLAKE_VERSION/starlake-spark3_2.12-$STARLAKE_VERSION-assembly.jar
RUN mkdir -p /samples/bin
RUN mv /app/starlake-spark3_2.12-$STARLAKE_VERSION-assembly.jar /samples/bin/
RUN mv /app/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /samples/bin/

COPY local/ /samples/local/
COPY quickstart-template/ /samples/quickstart-template/
COPY env.sh /samples/

COPY docker-init.sh /app

WORKDIR /samples/local

CMD ["/app/docker-init.sh"]
