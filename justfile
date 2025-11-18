# Build and push the OTC server Docker image
docker-release:
    #!/usr/bin/env bash
    set -e
    VERSION_TAG=$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select(.name=="otc-server") | .version')
    echo "Checking version: ${VERSION_TAG}"
    
    # Check if the version tag already exists in Docker Hub
    if docker manifest inspect riftresearch/otc-server:${VERSION_TAG} > /dev/null 2>&1; then
        echo "Error: Version ${VERSION_TAG} already exists in Docker Hub"
        echo "Please update the version in Cargo.toml before releasing"
        exit 1
    fi
    
    echo "Building Docker image for version: ${VERSION_TAG}"
    docker build -f etc/Dockerfile.otc -t riftresearch/otc-server:${VERSION_TAG} .
    docker tag riftresearch/otc-server:${VERSION_TAG} riftresearch/otc-server:latest
    docker push riftresearch/otc-server:${VERSION_TAG}
    docker push riftresearch/otc-server:latest
