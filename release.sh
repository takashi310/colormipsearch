#!/bin/sh

# Exit after any error
set -o errexit

RELEASE_VER=$1

if [ "$RELEASE_VER" == "" ]; then
    echo "Usage: release.sh <version>"
    exit 1
fi

echo "Changing version numbers to ${RELEASE_VER}"
mvn versions:set -DnewVersion=${RELEASE_VER} -DgenerateBackupPoms=false
git commit -a -m "Updated version to ${RELEASE_VER}"

echo "Creating git tag for ${RELEASE_VER}"
git tag ${RELEASE_VER}

echo "Deploy maven artifacts"
mvn deploy

echo "Pushing to Github..."
git push origin
git push origin ${RELEASE_VER}
