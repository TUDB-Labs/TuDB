#!/usr/bin/env sh
set -eu

git checkout gh-pages
git checkout main -- docs/
mkdocs build
rm -rf docs
mv site docs
git add -A
git commit -m "Update docs"
git push origin gh-pages
git checkout -
