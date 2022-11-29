# First Stage - Linting
FROM hadolint/hadolint:latest-alpine as config
WORKDIR /
COPY Dockerfile .
# Execute the linting process
RUN echo "### Linting Dockerfile ###" && /bin/hadolint Dockerfile

# Second stage - Build target image
FROM ubuntu:22.10

ARG COMET_VERSION
ARG SPARK_VERSION
ARG HADOOP_VERSION


# Install Java.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get -y install openjdk-17-jdk curl jq --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app/starlake/
WORKDIR /app/starlake
COPY starlake.sh /app/starlake

RUN /app/starlake/starlake.sh

ENTRYPOINT ["/app/starlake/starlake.sh"]
