#!/usr/bin/env bash
# Script to update HUDI pom version and fix the various drogon files automatically.

# Exit on any error, undefined variables, and pipe failures
set -euo pipefail

# retrieve current pom version
CURRENT_VERSION=`mvn help:evaluate -Dexpression=project.version -q -DforceStdout | sed 's/\x1b\[[0-9;]*m//g'`


# increment the version
# Handle version suffix like -SNAPSHOT
VERSION_BASE="${CURRENT_VERSION%%-*}"
VERSION_SUFFIX=""
if [[ "$CURRENT_VERSION" == *-* ]]; then
    VERSION_SUFFIX="-${CURRENT_VERSION#*-}"
fi

IFS='.' read -a tokens <<< "$VERSION_BASE"
last_index=${#tokens[@]}-1
tokens[$last_index]=$(expr ${tokens[$last_index]} + 1)
NEW_VERSION="$(IFS=. ; echo "${tokens[*]}")${VERSION_SUFFIX}"


# accept choice
echo ""
while true; do
    read -p "Do you wish to upgrade HUDI from $CURRENT_VERSION to $NEW_VERSION [yNe(dit)]? " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        [eE]* ) read -p "Please enter the new HUDI version (current=$CURRENT_VERSION): " NEW_VERSION;;
        * ) exit;;
    esac
done


# Upgrade HUDI version in the various json files for drogon
echo ""
if ls drogon/*.json 1>/dev/null 2>&1; then
    DROGON_FILES=($(grep 'HUDI_VERSION": ' --files-with-matches drogon/*.json 2>/dev/null || true))
    if [[ ${#DROGON_FILES[@]} -gt 0 ]]; then
        search='"HUDI_VERSION": ".*"'
        replace='"HUDI_VERSION": "'"$NEW_VERSION"'"'
        if [[ "$OSTYPE" == "darwin"* ]]; then
            SED_INPLACE_OPTION=(-i "")
        else
            SED_INPLACE_OPTION=(-i)
        fi

        for file in "${DROGON_FILES[@]}"
        do
            echo "Updating HUDI version in drogon file $file"
            sed "${SED_INPLACE_OPTION[@]}" "s/$search/$replace/g" $file
        done
    else
        echo "No drogon files contain HUDI_VERSION, skipping..."
    fi
else
    echo "No drogon/*.json files found, skipping..."
fi

echo "Upgrading HUDI pom to version $NEW_VERSION"
mvn versions:set -DgenerateBackupPoms=false -DnewVersion=$NEW_VERSION 1>/dev/null


# show diff if required
echo ""
while true; do
    read -p "Show diff of changes [yN]? " yn
    case $yn in
        [Yy]* ) git diff && break;;
        * ) break;;
    esac
done


# commit and push changes
echo ""
while true; do
    read -p "Commit changes and push to branch [yN]? " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) exit;;
    esac
done


# commit changes
git add -u
git commit -m "[UBER] Upgrade version to $NEW_VERSION" 1>/dev/null
echo ""


# push the branch
git push 1>/dev/null
if [ $? -ne 0 ]; then
    echo "Failed to push the branch"
else
    echo "All done. Publish the new version to artifactory at https://engwiki.uberinternal.com/display/HUDI/Upload+hudi+artifacts+to+artifactory"
fi
