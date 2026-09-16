#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
LISTEN=${AGENT_DEV_LISTEN:-127.0.0.1:8080}
NGROK_DOMAIN=${NGROK_DOMAIN:-streak-upscale-okay.ngrok-free.dev}
PUBLIC_URL="https://$NGROK_DOMAIN"
AGENT_GCP_PROJECT=${AGENT_GCP_PROJECT:-project-2c02a9f6-1ff5-461d-ae0}
IMAGE_BACKEND=${AGENT_IMAGE_BACKEND:-gcs}
IMAGE_BUCKET=${AGENT_IMAGE_BUCKET:-agent-platform-images-361197090477}
EXECUTION_CONNECTION_ID=${AGENT_EXECUTION_CONNECTION_ID:-execution-primary}
EXECUTION_GATEWAY_URL=${AGENT_EXECUTION_GATEWAY_URL:-https://execution.acentric.dev/}
EXECUTION_API_KEY_SECRET=${AGENT_EXECUTION_API_KEY_SECRET:-agent-server-execution-gateway-api-key}
EXECUTION_WEBHOOK_SECRET=${AGENT_EXECUTION_WEBHOOK_SECRET:-agent-server-execution-gateway-webhook-secret}
LLM_CONNECTION_ID=${AGENT_LLM_CONNECTION_ID:-llm-primary}
LLM_GATEWAY_URL=${AGENT_LLM_GATEWAY_URL:-https://gateway.acentric.dev/}
LLM_API_KEY_SECRET=${AGENT_LLM_API_KEY_SECRET:-agent-server-llm-gateway-api-key}
LLM_WEBHOOK_SECRET=${AGENT_LLM_WEBHOOK_SECRET:-agent-server-llm-gateway-webhook-secret}
SERVER_PID=
NGROK_PID=
LOG_DIR=

fail() {
  echo "error: $*" >&2
  exit 1
}

stop_process() {
  pid=$1
  [ -n "$pid" ] || return 0
  kill -0 "$pid" 2>/dev/null || return 0
  kill "$pid" 2>/dev/null || true
  attempts=0
  while kill -0 "$pid" 2>/dev/null && [ "$attempts" -lt 50 ]; do
    sleep 0.1
    attempts=$((attempts + 1))
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill -KILL "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
}

cleanup() {
  status=$?
  trap - EXIT INT TERM HUP
  stop_process "$NGROK_PID"
  stop_process "$SERVER_PID"
  if [ -n "$LOG_DIR" ] && [ -d "$LOG_DIR" ]; then
    rm -f "$LOG_DIR/ngrok.log"
    rmdir "$LOG_DIR" 2>/dev/null || true
  fi
  exit "$status"
}

interrupted() {
  exit 130
}

trap cleanup EXIT
trap interrupted INT TERM HUP

case "$NGROK_DOMAIN" in
  ""|*/*|*:*|*[!A-Za-z0-9.-]*)
    fail "NGROK_DOMAIN must be a hostname without a scheme or path"
    ;;
esac

case "$LISTEN" in
  ""|*[!A-Za-z0-9.:-]*)
    fail "AGENT_DEV_LISTEN must be an address such as 127.0.0.1:8080"
    ;;
esac

: "${AGENT_DATABASE_URL:?Set AGENT_DATABASE_URL to the agent-platform PostgreSQL database URL}"
: "${AGENT_SERVICE_TOKENS:?Set AGENT_SERVICE_TOKENS to one or more comma-separated backend tokens}"

command -v cargo >/dev/null 2>&1 || fail "cargo was not found"
command -v curl >/dev/null 2>&1 || fail "curl was not found"

case "$IMAGE_BACKEND" in
  local)
    ;;
  gcs)
    command -v gcloud >/dev/null 2>&1 || fail "gcloud is required for the GCS image backend"
    [ -n "$IMAGE_BUCKET" ] || fail "AGENT_IMAGE_BUCKET is required for the GCS image backend"
    gcloud auth application-default print-access-token >/dev/null 2>&1 \
      || fail "Google Application Default Credentials are missing; run 'gcloud auth application-default login'"
    ;;
  *)
    fail "AGENT_IMAGE_BACKEND must be local or gcs"
    ;;
esac
export AGENT_IMAGE_BACKEND=$IMAGE_BACKEND
export AGENT_IMAGE_BUCKET=$IMAGE_BUCKET

if [ "${AGENT_EXECUTION_GATEWAY_AUTO_CONFIG:-true}" = true ]; then
  command -v gcloud >/dev/null 2>&1 || fail "gcloud is required for automatic execution-gateway configuration"
  command -v jq >/dev/null 2>&1 || fail "jq is required for automatic execution-gateway configuration"

  connections=${AGENT_GATEWAY_CONNECTIONS:-[]}
  callbacks=${AGENT_GATEWAY_CALLBACKS:-[]}
  printf '%s' "$connections" | jq -e 'type == "array"' >/dev/null \
    || fail "AGENT_GATEWAY_CONNECTIONS must be a JSON array"
  printf '%s' "$callbacks" | jq -e 'type == "array"' >/dev/null \
    || fail "AGENT_GATEWAY_CALLBACKS must be a JSON array"

  if ! printf '%s' "$connections" | jq -e --arg id "$EXECUTION_CONNECTION_ID" \
    'any(.[]; .id == $id)' >/dev/null; then
    execution_api_key=$(gcloud secrets versions access latest \
      --project="$AGENT_GCP_PROJECT" --secret="$EXECUTION_API_KEY_SECRET") \
      || fail "could not load the execution-gateway API key from Secret Manager"
    connections=$(printf '%s' "$connections" | jq -c \
      --arg id "$EXECUTION_CONNECTION_ID" \
      --arg url "$EXECUTION_GATEWAY_URL" \
      --arg token "$execution_api_key" \
      '. + [{id:$id,kind:"execution",base_url:$url,bearer_token:$token,timeout_ms:30000}]')
    unset execution_api_key
  fi

  if ! printf '%s' "$callbacks" | jq -e --arg id "$EXECUTION_CONNECTION_ID" \
    'any(.[]; .id == $id)' >/dev/null; then
    execution_webhook_secret=$(gcloud secrets versions access latest \
      --project="$AGENT_GCP_PROJECT" --secret="$EXECUTION_WEBHOOK_SECRET") \
      || fail "could not load the execution-gateway webhook secret from Secret Manager"
    callbacks=$(printf '%s' "$callbacks" | jq -c \
      --arg id "$EXECUTION_CONNECTION_ID" \
      --arg secret "$execution_webhook_secret" \
      '. + [{id:$id,kind:"execution",secrets:[$secret]}]')
    unset execution_webhook_secret
  fi

  export AGENT_GATEWAY_CONNECTIONS="$connections"
  export AGENT_GATEWAY_CALLBACKS="$callbacks"
fi

if [ "${AGENT_LLM_GATEWAY_AUTO_CONFIG:-true}" = true ]; then
  command -v gcloud >/dev/null 2>&1 || fail "gcloud is required for automatic LLM-gateway configuration"
  command -v jq >/dev/null 2>&1 || fail "jq is required for automatic LLM-gateway configuration"

  connections=${AGENT_GATEWAY_CONNECTIONS:-[]}
  callbacks=${AGENT_GATEWAY_CALLBACKS:-[]}
  printf '%s' "$connections" | jq -e 'type == "array"' >/dev/null \
    || fail "AGENT_GATEWAY_CONNECTIONS must be a JSON array"
  printf '%s' "$callbacks" | jq -e 'type == "array"' >/dev/null \
    || fail "AGENT_GATEWAY_CALLBACKS must be a JSON array"

  if ! printf '%s' "$connections" | jq -e --arg id "$LLM_CONNECTION_ID" \
    'any(.[]; .id == $id)' >/dev/null; then
    llm_api_key=$(gcloud secrets versions access latest \
      --project="$AGENT_GCP_PROJECT" --secret="$LLM_API_KEY_SECRET") \
      || fail "could not load the LLM-gateway API key from Secret Manager"
    connections=$(printf '%s' "$connections" | jq -c \
      --arg id "$LLM_CONNECTION_ID" \
      --arg url "$LLM_GATEWAY_URL" \
      --arg token "$llm_api_key" \
      '. + [{id:$id,kind:"llm",base_url:$url,bearer_token:$token,timeout_ms:30000}]')
    unset llm_api_key
  fi

  if ! printf '%s' "$callbacks" | jq -e --arg id "$LLM_CONNECTION_ID" \
    'any(.[]; .id == $id)' >/dev/null; then
    llm_webhook_secret=$(gcloud secrets versions access latest \
      --project="$AGENT_GCP_PROJECT" --secret="$LLM_WEBHOOK_SECRET") \
      || fail "could not load the LLM-gateway webhook secret from Secret Manager"
    callbacks=$(printf '%s' "$callbacks" | jq -c \
      --arg id "$LLM_CONNECTION_ID" \
      --arg secret "$llm_webhook_secret" \
      '. + [{id:$id,kind:"llm",secrets:[$secret]}]')
    unset llm_webhook_secret
  fi

  export AGENT_GATEWAY_CONNECTIONS="$connections"
  export AGENT_GATEWAY_CALLBACKS="$callbacks"
fi

if [ -n "${NGROK_BIN:-}" ]; then
  NGROK_EXEC=$NGROK_BIN
else
  NGROK_EXEC=$(command -v ngrok 2>/dev/null || true)
  if command -v python3 >/dev/null 2>&1; then
    PYNGROK_EXEC=$(python3 -c 'from pyngrok import conf; print(conf.get_default().ngrok_path)' 2>/dev/null || true)
    if [ -n "$PYNGROK_EXEC" ] && [ -x "$PYNGROK_EXEC" ]; then
      NGROK_EXEC=$PYNGROK_EXEC
    fi
  fi
fi
[ -n "$NGROK_EXEC" ] && [ -x "$NGROK_EXEC" ] || fail "ngrok was not found; set NGROK_BIN if it is installed elsewhere"
"$NGROK_EXEC" config check >/dev/null 2>&1 || fail "ngrok is not configured or authenticated"

if [ "${AGENT_GATEWAY_CALLBACKS:-[]}" = "[]" ]; then
  echo "warning: AGENT_GATEWAY_CALLBACKS is empty; signed gateway callbacks will not be accepted" >&2
fi

cd "$ROOT"

if [ -n "${CARGO_TARGET_DIR:-}" ]; then
  case "$CARGO_TARGET_DIR" in
    /*) TARGET_DIR=$CARGO_TARGET_DIR ;;
    *) TARGET_DIR="$ROOT/$CARGO_TARGET_DIR" ;;
  esac
else
  TARGET_DIR="$ROOT/target"
fi
SERVER_BIN="$TARGET_DIR/debug/agent-server"

echo "Building agent-server..."
cargo build --package agent-server

echo "Applying agent-platform migrations..."
"$SERVER_BIN" migrate

export AGENT_LISTEN=$LISTEN
echo "Starting agent-server on http://$LISTEN..."
"$SERVER_BIN" serve &
SERVER_PID=$!

attempts=0
until curl --fail --silent --show-error "http://$LISTEN/readyz" >/dev/null 2>&1; do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" || true
    fail "agent-server exited before becoming ready"
  fi
  attempts=$((attempts + 1))
  [ "$attempts" -lt 120 ] || fail "agent-server did not become ready within 60 seconds"
  sleep 0.5
done

LOG_DIR=$(mktemp -d "${TMPDIR:-/tmp}/agent-server-ngrok.XXXXXX")
echo "Starting ngrok tunnel at $PUBLIC_URL..."
"$NGROK_EXEC" http --url="$NGROK_DOMAIN" "$LISTEN" \
  --log "$LOG_DIR/ngrok.log" --log-format json &
NGROK_PID=$!

attempts=0
until curl --fail --silent --show-error \
  --header 'ngrok-skip-browser-warning: true' "$PUBLIC_URL/healthz" >/dev/null 2>&1; do
  if ! kill -0 "$NGROK_PID" 2>/dev/null; then
    wait "$NGROK_PID" || true
    if [ -s "$LOG_DIR/ngrok.log" ]; then
      tail -20 "$LOG_DIR/ngrok.log" >&2
    fi
    fail "ngrok exited before the public endpoint became ready"
  fi
  attempts=$((attempts + 1))
  [ "$attempts" -lt 120 ] || fail "ngrok endpoint did not become ready within 60 seconds"
  sleep 0.5
done

echo
echo "Agent server:       http://$LISTEN"
echo "Public HTTPS base:  $PUBLIC_URL"
if [ "$IMAGE_BACKEND" = gcs ]; then
  echo "Image bucket:       gs://$IMAGE_BUCKET"
else
  echo "Image origin:       ${AGENT_IMAGE_PUBLIC_BASE_URL:-http://127.0.0.1:8080}"
fi
echo "LLM callback:       $PUBLIC_URL/v1/callbacks/llm/<connection-id>"
echo "Execution callback: $PUBLIC_URL/v1/callbacks/execution/<connection-id>"
echo
echo "The public URL is fixed by --url and will be the same on later runs."
echo "Press Ctrl-C to stop agent-server and ngrok."

while kill -0 "$SERVER_PID" 2>/dev/null && kill -0 "$NGROK_PID" 2>/dev/null; do
  sleep 1
done

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
  if wait "$SERVER_PID"; then
    fail "agent-server stopped unexpectedly"
  else
    status=$?
    echo "error: agent-server exited with status $status" >&2
    exit "$status"
  fi
fi

if wait "$NGROK_PID"; then
  fail "ngrok stopped unexpectedly"
else
  status=$?
  echo "error: ngrok exited with status $status" >&2
  if [ -s "$LOG_DIR/ngrok.log" ]; then
    tail -20 "$LOG_DIR/ngrok.log" >&2
  fi
  exit "$status"
fi
