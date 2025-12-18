#!/bin/bash
set -e

# ByteFreezer Query Build Script
# Builds the binary for local Ansible deployment

PROJECT_NAME="bytefreezer-query"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ANSIBLE_DIST_DIR="$SCRIPT_DIR/ansible/playbooks/dist"

echo "Building $PROJECT_NAME..."

# Create ansible/playbooks/dist directory
mkdir -p "$ANSIBLE_DIST_DIR"

# Get version info
VERSION="local-$(date +%Y%m%d-%H%M%S)"
BUILD_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

echo "Version: $VERSION"
echo "Build Time: $BUILD_TIME"
echo "Git Commit: $GIT_COMMIT"

# Remove old binary if it exists
if [[ -f "$ANSIBLE_DIST_DIR/$PROJECT_NAME" ]]; then
    echo "Removing old binary..."
    rm -f "$ANSIBLE_DIST_DIR/$PROJECT_NAME"
fi

# Build the binary (CGO_ENABLED=1 required for DuckDB)
echo "Building binary..."
CGO_ENABLED=1 go build \
    -ldflags="-s -w -X main.version=$VERSION -X main.buildTime=$BUILD_TIME -X main.gitCommit=$GIT_COMMIT" \
    -o "$ANSIBLE_DIST_DIR/$PROJECT_NAME" \
    .

# Verify binary was created
if [[ ! -f "$ANSIBLE_DIST_DIR/$PROJECT_NAME" ]]; then
    echo "Error: Binary not found at $ANSIBLE_DIST_DIR/$PROJECT_NAME"
    exit 1
fi

# Copy UI files
echo "Copying UI files..."
mkdir -p "$ANSIBLE_DIST_DIR/ui"
cp -r "$SCRIPT_DIR/ui/"* "$ANSIBLE_DIST_DIR/ui/"

# Copy prompts
echo "Copying prompts..."
mkdir -p "$ANSIBLE_DIST_DIR/prompts"
cp -r "$SCRIPT_DIR/prompts/"* "$ANSIBLE_DIST_DIR/prompts/"

echo "Build successful!"
echo "Binary location: $ANSIBLE_DIST_DIR/$PROJECT_NAME"
echo ""
echo "Dist contents:"
ls -la "$ANSIBLE_DIST_DIR/"
echo ""
echo "UI files:"
ls -la "$ANSIBLE_DIST_DIR/ui/"
echo ""
echo "Prompt files:"
ls -la "$ANSIBLE_DIST_DIR/prompts/"
