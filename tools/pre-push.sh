#!/bin/bash

set -euo pipefail

print_start() {
  echo -e "\n\033[1;33m=============================="
  echo -e "  🚧 RUNNING PRE-PUSH HOOK  "
  echo -e "==============================\033[0m\n"
}

print_success() {
  echo -e "\n\033[1;32m=============================="
  echo -e "  ✅ Pre-Push hook passed."
  echo -e "==============================\033[0m\n"
}

print_failure() {
  echo -e "\n\033[1;31m=============================="
  echo -e "  ❌ Pre-Push hook failed."
  echo -e "==============================\033[0m\n"
}

trap 'echo -e "\n💥 An unexpected error occurred. Aborting push."; print_failure; exit 1' ERR

print_start

echo -e "\033[1;36m>> Running full repository lint gate...\033[0m\n"
before_lint_diff_hash=$(git diff | git hash-object --stdin)
if ! make lint; then
  echo -e "\n❌ Push blocked because repository lint failed.\n"
  print_failure
  exit 1
fi

after_lint_diff_hash=$(git diff | git hash-object --stdin)
if [[ "$before_lint_diff_hash" != "$after_lint_diff_hash" ]]; then
  echo -e "\n❌ Push blocked because lint formatting changed files:\n"
  git diff --name-only | sed $'s/^/\t>> /'
  echo -e "\n💡 Review and commit these changes, then push again.\n"
  print_failure
  exit 1
fi

echo -e "\n\033[1;36m>> Running full repository test coverage gate...\033[0m\n"
if ! make testcoverage; then
  echo -e "\n❌ Push blocked because repository tests or coverage failed.\n"
  print_failure
  exit 1
fi

print_success
exit 0
