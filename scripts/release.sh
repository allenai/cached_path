#!/bin/bash

set -e

TAG=$(python -c 'from cached_path.version import VERSION; print("v" + VERSION)')
git commit -a -m "Bump version to $TAG for release" || true && git push
echo "Creating new git tag $TAG"
git tag "$TAG" -m "$TAG"
git push --tags
