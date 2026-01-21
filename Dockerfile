FROM eclipse-temurin:21

ARG BUILD_DATE
ARG VCS_REF
ARG SL_VERSION

# Labels.
LABEL org.label-schema.schema-version="1.0"
LABEL org.label-schema.build-date=$BUILD_DATE
LABEL org.label-schema.vendor="starlakeai"
LABEL org.label-schema.name="starlakeai/starlake"
LABEL org.label-schema.version=$SL_VERSION
LABEL org.label-schema.description="A declarative text based tool that enables analysts and engineers to extract, load, transform and orchestrate their data pipelines."
LABEL org.label-schema.url="https://starlake.ai"
LABEL org.label-schema.vcs-url="https://github.com/starlake-ai/starlake"
LABEL org.label-schema.vcs-ref=$VCS_REF

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        findutils \
        jq \
        graphviz \
        unzip \
        curl \
        bash \
        vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Allow arbitrary UID
RUN chgrp -R 0 /app && chmod -R g=u /app && \
    chmod 755 /app/starlake.sh && \
    useradd starlake -u 1001 -g 0 -m -s /bin/bash

USER 1001

COPY starlake/bin /app/bin
COPY starlake/starlake.sh /app/starlake.sh
COPY starlake/versions.sh /app/versions.sh

ENTRYPOINT ["/app/starlake.sh"]
