# Use the Official OpenJDK image for a lean production stage of our multi-stage build.
# https://hub.docker.com/_/openjdk
# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
FROM openjdk:8-jre-alpine
ARG STARLAKE_VERSION=0.2.8

RUN apk add --update \
    curl \
    && rm -rf /var/cache/apk/*

RUN apk update && apk add bash

WORKDIR /app
## configure spark
RUN curl -L -O https://downloads.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
RUN tar -zxf spark-3.2.1-bin-hadoop3.2.tgz -C /app
RUN rm spark-3.2.1-bin-hadoop3.2.tgz

## download starlake
RUN curl -L -O https://s01.oss.sonatype.org/content/repositories/releases/ai/starlake/starlake-spark3_2.12/$STARLAKE_VERSION/starlake-spark3_2.12-$STARLAKE_VERSION-assembly.jar
RUN mkdir -p /samples/bin
RUN mv /app/starlake-spark3_2.12-$STARLAKE_VERSION-assembly.jar /samples/bin/
RUN mv /app/spark-3.2.1-bin-hadoop3.2 /samples/bin/

COPY local/ /samples/local/
COPY quickstart-template/ /samples/quickstart-template/
COPY env.sh /samples/

COPY docker-init.sh /app

WORKDIR /samples/local

CMD ["/app/docker-init.sh"]
