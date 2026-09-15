#!/bin/sh
set -eu

if [ -z "${TEST_DATABASE_URL:-}" ]; then
  echo 'Set TEST_DATABASE_URL to a disposable-test PostgreSQL admin database.' >&2
  exit 2
fi

cd "$(dirname "$0")/.."
cargo fmt --all -- --check
cargo test --workspace
cargo test --workspace -- --ignored --test-threads=1
