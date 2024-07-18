docker buildx use multi-arch-builder
docker buildx inspect --bootstrap
docker buildx build --platform linux/amd64,linux/arm64  --build-arg SCALA_VERSION=2.13 -t starlakeai/starlake:latest .
