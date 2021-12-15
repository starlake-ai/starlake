# Use the Official OpenJDK image for a lean production stage of our multi-stage build.
# https://hub.docker.com/_/openjdk
# https://docs.docker.com/develop/develop-images/multistage-build/#use-multi-stage-builds
FROM openjdk:8-jre-alpine

RUN apk add --update \
    curl \
    && rm -rf /var/cache/apk/*

RUN apk update && apk add bash
# Copy local code to the container image.
WORKDIR /app
## configure spark
RUN curl -L -O https://downloads.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
RUN tar -zxf spark-3.1.2-bin-hadoop3.2.tgz -C /app
RUN rm spark-3.1.2-bin-hadoop3.2.tgz



# Copy the jar to the production image from the builder stage.

RUN curl -L -O https://s01.oss.sonatype.org/content/repositories/snapshots/ai/starlake/starlake-spark3_2.12/0.2.8-SNAPSHOT/starlake-spark3_2.12-0.2.8-SNAPSHOT-assembly.jar

# Run the web service on container startup.
#CMD ["java", "-cp","/starlake.jar:/lib/*", "ai.starlake.services.launch.Application"]

COPY docker-init.sh /app

CMD ["/app/docker-init.sh"]
