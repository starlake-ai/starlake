FROM eclipse-temurin:21

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

COPY starlake/bin /app/bin
COPY starlake/starlake.sh /app/starlake.sh
COPY starlake/versions.sh /app/versions.sh

ENTRYPOINT ["/app/starlake.sh"]
