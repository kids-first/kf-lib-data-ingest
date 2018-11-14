#!/bin/bash
# Based on https://github.com/Villanuevand/deployment-circleci-gh-pages

# Abort the script if there is a non-zero error
set -e

# Show where we are on the machine
pwd

# Delete gh-pages-branch if it exists
if [ -d "gh-pages-branch" ]; then
  rm -rf gh-pages-branch
fi

# Get remote
remote=$(git config remote.origin.url)

# Move built static site to tmp dir
mkdir ../tmp_docs
shopt -s dotglob
mv -fv docs/build/html/* ../tmp_docs
# Future - Add other doc dependencies like code examples folder

# Make a directory to put the gp-pages branch
mkdir gh-pages-branch
cd gh-pages-branch

# Now lets setup a new repo so we can update the gh-pages branch
git config --global user.email "$GH_EMAIL" > /dev/null 2>&1
git config --global user.name "$GH_NAME" > /dev/null 2>&1
git init
git remote add --fetch origin "$remote"

# Switch into the gh-pages branch
if git rev-parse --verify origin/gh-pages > /dev/null 2>&1
then
    git checkout gh-pages
    # delete any old site as we are going to replace it
    # Note: this explodes if there aren't any, so moving it here for now
    git rm -rf .
else
    git checkout --orphan gh-pages
fi

# Move built docs to gh-pages root dir
mv -fv ../../tmp_docs/* .

# Stage any changes and new files
git add -A

# Now commit, ignoring branch gh-pages doesn't seem to work, so trying skip
git commit --allow-empty -m "Deploy to GitHub pages [ci skip]"

# And push, but send any output to /dev/null to hide anything sensitive
git push --force --quiet origin gh-pages > /dev/null 2>&1

# Clean up
cd ..
rm -rf gh-pages-branch
rm -rf ../tmp_docs

echo "Finished Deployment!"
