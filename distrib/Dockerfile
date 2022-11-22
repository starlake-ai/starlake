# Use the Official OpenJDK image for a lean production stage of our multi-stage build.
# https://hub.docker.com/_/openjdk
# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
# Pull base image.
FROM ubuntu

ARG COMET_VERSION=0.5.3
ARG SPARK_VERSION=3.3.1
ARG HADOOP_VERSION=3


# Install Java.
RUN apt-get update && apt-get -y install openjdk-17-jdk curl


RUN mkdir -p /app/starlake/
WORKDIR /app/starlake
COPY starlake.sh /app/starlake

RUN /app/starlake/starlake.sh

ENTRYPOINT ["/app/starlake/starlake.sh"]
