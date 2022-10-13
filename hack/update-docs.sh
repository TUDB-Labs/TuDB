#!/usr/bin/env sh
set -eu

git checkout gh-pages
git checkout main -- docs/
mkdocs build
rm -rf docs
mv site docs
