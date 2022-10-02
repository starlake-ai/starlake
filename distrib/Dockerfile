# Use the Official OpenJDK image for a lean production stage of our multi-stage build.
# https://hub.docker.com/_/openjdk
# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
# Pull base image.
FROM ubuntu

ARG STARLAKE_VERSION=0.3.27-SNAPSHOT
ARG SPARK_VERSION=3.3.0
ARG HADOOP_VERSION=3

# Install Java.
RUN apt-get update && apt-get -y install openjdk-8-jdk curl
#RUN update-alternatives --set java /usr/lib/jvm/jdk1.8.0_version/bin/java


RUN mkdir -p /app/starlake/
WORKDIR /app/starlake
COPY starlake.sh /app/starlake

RUN /app/starlake/starlake.sh

ENTRYPOINT ["/app/starlake/starlake.sh"]
