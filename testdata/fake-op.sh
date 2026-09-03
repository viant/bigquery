#!/bin/sh
# Test double for the 1Password CLI. Used by unit tests so `op read` never
# touches a real 1Password session or prompts for access.
if [ "$1" != "read" ]; then
  echo "fake-op: unsupported command: $1" >&2
  exit 1
fi
printf '%s' '{"type":"service_account","project_id":"test"}'
