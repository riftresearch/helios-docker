# Show current version
version:
    #!/usr/bin/env bash
    HELIOS_VERSION=$(cd helios && cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select(.name=="helios-cli") | .version')
    PATCH_VERSION=$(cat PATCH)
    echo "Current version: ${HELIOS_VERSION}-${PATCH_VERSION}"
    echo "  Helios: ${HELIOS_VERSION}"
    echo "  Patch:  ${PATCH_VERSION}"

# Increment patch version
bump-patch:
    #!/usr/bin/env bash
    CURRENT_PATCH=$(cat PATCH)
    NEW_PATCH=$((CURRENT_PATCH + 1))
    echo ${NEW_PATCH} > PATCH
    HELIOS_VERSION=$(cd helios && cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select(.name=="helios-cli") | .version')
    echo "Bumped patch version: ${CURRENT_PATCH} → ${NEW_PATCH}"
    echo "New version will be: ${HELIOS_VERSION}-${NEW_PATCH}"

# Sync helios subtree to latest upstream
sync-helios:
    #!/usr/bin/env bash
    echo "Syncing helios to latest..."
    git subtree pull --prefix helios https://github.com/riftresearch/helios.git master --squash
    echo "Helios updated to latest"

# Build helios Docker image locally
docker-build:
    #!/usr/bin/env bash
    set -e
    HELIOS_VERSION=$(cd helios && cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select(.name=="helios-cli") | .version')
    PATCH_VERSION=$(cat PATCH)
    VERSION_TAG="${HELIOS_VERSION}-${PATCH_VERSION}"
    echo "Building helios Docker image version: ${VERSION_TAG}"
    echo "  (helios: ${HELIOS_VERSION}, patch: ${PATCH_VERSION})"
    docker build -t riftresearch/helios:${VERSION_TAG} -t riftresearch/helios:latest .
    echo "Image built: riftresearch/helios:${VERSION_TAG}"

# Build and push helios Docker image
docker-release:
    #!/usr/bin/env bash
    set -e
    HELIOS_VERSION=$(cd helios && cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select(.name=="helios-cli") | .version')
    PATCH_VERSION=$(cat PATCH)
    VERSION_TAG="${HELIOS_VERSION}-${PATCH_VERSION}"
    echo "Checking version: ${VERSION_TAG}"
    echo "  (helios: ${HELIOS_VERSION}, patch: ${PATCH_VERSION})"
    
    # Check if the version tag already exists in Docker Hub
    if docker manifest inspect riftresearch/helios:${VERSION_TAG} > /dev/null 2>&1; then
        echo "Error: Version ${VERSION_TAG} already exists in Docker Hub"
        echo "Please increment the PATCH file before releasing"
        exit 1
    fi
    
    echo "Building Docker image for version: ${VERSION_TAG}"
    docker build -t riftresearch/helios:${VERSION_TAG} -t riftresearch/helios:latest .
    docker push riftresearch/helios:${VERSION_TAG}
    docker push riftresearch/helios:latest
    echo "Released riftresearch/helios:${VERSION_TAG}"
