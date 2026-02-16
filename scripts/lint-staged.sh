#!/bin/sh
# This is to get functionality similar to the lint-staged package, but simpler and faster.
files="$(git diff --cached --name-only --diff-filter=ACMR)"
[ -z "$files" ] && exit 0
echo "$files" | xargs pnpm prettier --check --ignore-unknown
