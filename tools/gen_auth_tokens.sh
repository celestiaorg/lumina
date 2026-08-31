#!/usr/bin/env bash
set -euo pipefail

DOTENV=".env"
DOTENV_SAMPLE=".env.sample"
DOCKER_COMPOSE_FILE="./ci/docker-compose.yml"

# how long to wait for all services to become healthy, in seconds
WAIT_TIMEOUT="${WAIT_TIMEOUT:-180}"

wait_for_docker_setup() {
  local services_expected
  services_expected="$(docker compose -f "$DOCKER_COMPOSE_FILE" config --services | wc -l)"

  printf "Waiting for docker compose services"

  for (( elapsed = 0; elapsed <= WAIT_TIMEOUT; elapsed++ )); do
    local status
    local healthy
    local starting

    status="$(docker compose -f "$DOCKER_COMPOSE_FILE" ps --format '{{.Health}}')"
    healthy="$(echo "$status" | grep -c "^healthy")" || true
    starting="$(echo "$status" | grep -c "^starting")" || true


    if (( healthy == services_expected )); then
      break
    elif (( healthy + starting != services_expected )); then
      echo ""
      echo "Some services crashed or are unhealthy" >&2
      docker compose -f "$DOCKER_COMPOSE_FILE" ps --all
      docker compose -f "$DOCKER_COMPOSE_FILE" logs --tail 100
      exit 1
    fi

    printf "."
    sleep 1
  done

  if (( healthy != services_expected )); then
    echo ""
    echo "Services did not become healthy within ${WAIT_TIMEOUT}s" >&2
    docker compose -f "$DOCKER_COMPOSE_FILE" ps --all
    docker compose -f "$DOCKER_COMPOSE_FILE" logs --tail 100
    exit 1
  fi

  echo ""
  echo "All services healthy"
}

ensure_dotenv_file() {
  if [ ! -e "$DOTENV" ]; then
    if [ ! -e "$DOTENV_SAMPLE" ]; then
      echo "$DOTENV_SAMPLE file not found." \
        "Make sure to run this script from repository root" >&2
      exit 1
    fi

    echo "$DOTENV file not found, creating a new one"
    cp "$DOTENV_SAMPLE" "$DOTENV"
  else
    echo "Found existing $DOTENV file"
  fi
}

generate_token() {
  local auth_level="$1"
  local node_type
  node_type="$(docker compose -f "$DOCKER_COMPOSE_FILE" exec -T node-1 \
    ls -a /root | grep private | cut -d- -f 2)"
  docker compose -f "$DOCKER_COMPOSE_FILE" exec -T node-1 \
    celestia "$node_type" auth "$auth_level" --p2p.network private
}

write_token() {
  local auth_level="$1"
  local token="$2"

  auth_level="$(echo "$auth_level" | tr '[:lower:]' '[:upper:]')"

  local var_name="CELESTIA_NODE_AUTH_TOKEN_${auth_level}"

  echo "Saving token $var_name=$token"

  sed -i.bak "s/.*$var_name.*/$var_name=$token/" "$DOTENV"
  # there's no compatible way to tell sed not to do a backup file
  # accept it and remove the file afterwards
  rm "$DOTENV.bak"
}

main() {
  wait_for_docker_setup
  ensure_dotenv_file

  for auth_level in "read" "write" "admin"; do
    local token
    token="$(generate_token "$auth_level")"
    write_token "$auth_level" "$token"
  done
}

main
