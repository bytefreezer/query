#!/bin/bash
# Build script for ByteFreezer Query - prepares dist directory for AWX deployment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DIST_DIR="$SCRIPT_DIR/playbooks/dist"

echo "Building ByteFreezer Query for deployment..."
echo "Project directory: $PROJECT_DIR"
echo "Dist directory: $DIST_DIR"

# Create dist directory
mkdir -p "$DIST_DIR"

# Build the binary
echo "Building binary..."
cd "$PROJECT_DIR"
CGO_ENABLED=1 go build -o "$DIST_DIR/bytefreezer-query" .

# Copy UI files
echo "Copying UI files..."
mkdir -p "$DIST_DIR/ui"
cp -r "$PROJECT_DIR/ui/"* "$DIST_DIR/ui/"

# Copy prompts
echo "Copying prompts..."
mkdir -p "$DIST_DIR/prompts"
cp -r "$PROJECT_DIR/prompts/"* "$DIST_DIR/prompts/"

echo ""
echo "Build complete! Files in $DIST_DIR:"
ls -la "$DIST_DIR/"
echo ""
echo "UI files:"
ls -la "$DIST_DIR/ui/"
echo ""
echo "Prompt files:"
ls -la "$DIST_DIR/prompts/"
echo ""
echo "Ready for AWX deployment. Run:"
echo "  ansible-playbook -i inventory demo_install.yml"
