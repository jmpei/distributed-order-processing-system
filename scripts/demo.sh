#!/usr/bin/env bash
# Live demo of the saga orchestration: happy path, then compensation.
# Prereq: `make up` from project root, then `bash scripts/demo.sh`.
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "${SCRIPT_DIR}/.." && pwd )"
COMPOSE_DIR="${PROJECT_ROOT}/deploy"

ORDER_URL="${ORDER_URL:-http://localhost:8081}"
INVENTORY_URL="${INVENTORY_URL:-http://localhost:8082}"
PAYMENT_URL="${PAYMENT_URL:-http://localhost:8083}"

HAPPY_PRODUCT=9001
COMP_PRODUCT=9002
STOCK=10
QUANTITY=2
SAGA_WAIT=5
RESTART_WAIT=8

BOLD=$'\e[1m'; CYAN=$'\e[36m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'
RED=$'\e[31m'; DIM=$'\e[2m'; RESET=$'\e[0m'

banner() { printf "\n${BOLD}${CYAN}━━━ %s ━━━${RESET}\n" "$*"; }
step()   { printf "${BOLD}[%s]${RESET} %s\n" "$1" "$2"; }
info()   { printf "    ${DIM}%s${RESET}\n" "$*"; }
ok()     { printf "    ${GREEN}✓ %s${RESET}\n" "$*"; }
warn()   { printf "    ${YELLOW}! %s${RESET}\n" "$*"; }
fail()   { printf "    ${RED}✗ %s${RESET}\n" "$*"; exit 1; }
pause()  { sleep "${1:-1}"; }

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing dependency: $1"; exit 1; }
}

seed_product() {
  local pid=$1 qty=$2
  curl -s -o /dev/null -X POST "${INVENTORY_URL}/inventory" \
    -H 'Content-Type: application/json' \
    -d "{\"product_id\":${pid},\"available_qty\":${qty}}" || true
  curl -sf -o /dev/null -X PUT "${INVENTORY_URL}/inventory/${pid}" \
    -H 'Content-Type: application/json' \
    -d "{\"available_qty\":${qty}}"
}

place_order() {
  local pid=$1 qty=$2 amount=$3
  curl -sf -X POST "${ORDER_URL}/orders" \
    -H 'Content-Type: application/json' \
    -d "{\"user_id\":42,\"product_id\":${pid},\"quantity\":${qty},\"total_amount\":${amount}}"
}

# ── preflight ────────────────────────────────────────────────────────────────
require curl
require jq
require docker

banner "Preflight"
for url in "${ORDER_URL}/health" "${INVENTORY_URL}/health" "${PAYMENT_URL}/health"; do
  if curl -sf -o /dev/null "$url"; then
    ok "$url"
  else
    fail "$url unreachable — run 'make up' first"
  fi
done

restore_payment() {
  printf "\n"
  banner "Cleanup"
  step CLEAN "restoring payment-service to PAYMENT_FAILURE_RATE=0.0"
  ( cd "${COMPOSE_DIR}" && PAYMENT_FAILURE_RATE=0.0 \
      docker compose up -d --force-recreate payment-service > /dev/null )
  ok "payment-service restored"
}
trap restore_payment EXIT

# ── Phase 1: happy path ──────────────────────────────────────────────────────
banner "Phase 1 · Happy path  (order should reach CONFIRMED)"
pause

step 1 "seeding product ${HAPPY_PRODUCT} with ${STOCK} units"
seed_product "${HAPPY_PRODUCT}" "${STOCK}"
INV_BEFORE=$(curl -sf "${INVENTORY_URL}/inventory/${HAPPY_PRODUCT}" | jq -r .available_qty)
info "available_qty=${INV_BEFORE}"
pause

step 2 "POST /orders  (qty=${QUANTITY})"
RESP=$(place_order "${HAPPY_PRODUCT}" "${QUANTITY}" "199.98")
ORDER_ID=$(echo "${RESP}" | jq -r .order_id)
info "response: ${RESP}"
info "order_id=${ORDER_ID} — saga is now running across the 3 services"
pause

step 3 "waiting ${SAGA_WAIT}s for the saga to settle"
pause "${SAGA_WAIT}"

step 4 "checking final state"
ORDER_JSON=$(curl -sf "${ORDER_URL}/orders/${ORDER_ID}")
STATUS=$(echo "${ORDER_JSON}" | jq -r .status)
INV_AFTER=$(curl -sf "${INVENTORY_URL}/inventory/${HAPPY_PRODUCT}" | jq -r .available_qty)
PAY_STATUS=$(curl -sf "${PAYMENT_URL}/payments/${ORDER_ID}" | jq -r .status)
info "order:     ${ORDER_JSON}"
info "inventory: available ${INV_BEFORE} → ${INV_AFTER}"
info "payment:   ${PAY_STATUS}"
[ "${STATUS}" = "CONFIRMED" ]    || fail "expected CONFIRMED, got ${STATUS}"
[ "${PAY_STATUS}" = "SUCCESS" ]  || fail "expected payment SUCCESS, got ${PAY_STATUS}"
[ "${INV_AFTER}" = "$((INV_BEFORE - QUANTITY))" ] || fail "inventory not decremented correctly"
ok "happy path complete — order CONFIRMED, inventory decremented, payment SUCCESS"
pause 2

# ── Phase 2: compensation ────────────────────────────────────────────────────
banner "Phase 2 · Compensation  (force payment to fail, saga should COMPENSATE)"
pause

step 1 "restarting payment-service with PAYMENT_FAILURE_RATE=1.0"
( cd "${COMPOSE_DIR}" && PAYMENT_FAILURE_RATE=1.0 \
    docker compose up -d --force-recreate payment-service > /dev/null )
info "waiting ${RESTART_WAIT}s for payment-service to come back"
pause "${RESTART_WAIT}"

step 2 "seeding product ${COMP_PRODUCT} with ${STOCK} units"
seed_product "${COMP_PRODUCT}" "${STOCK}"
INV_BEFORE=$(curl -sf "${INVENTORY_URL}/inventory/${COMP_PRODUCT}" | jq -r .available_qty)
info "available_qty=${INV_BEFORE}"
pause

step 3 "POST /orders  (qty=${QUANTITY}, payment will fail → expect COMPENSATED)"
RESP=$(place_order "${COMP_PRODUCT}" "${QUANTITY}" "50.00")
ORDER_ID=$(echo "${RESP}" | jq -r .order_id)
info "order_id=${ORDER_ID}"
pause

step 4 "waiting ${SAGA_WAIT}s for compensation to land"
pause "${SAGA_WAIT}"

step 5 "checking final state"
ORDER_JSON=$(curl -sf "${ORDER_URL}/orders/${ORDER_ID}")
STATUS=$(echo "${ORDER_JSON}" | jq -r .status)
INV_AFTER=$(curl -sf "${INVENTORY_URL}/inventory/${COMP_PRODUCT}" | jq -r .available_qty)
info "order:     ${ORDER_JSON}"
info "inventory: available ${INV_BEFORE} → ${INV_AFTER} (should match)"
[ "${STATUS}" = "COMPENSATED" ]  || fail "expected COMPENSATED, got ${STATUS}"
[ "${INV_AFTER}" = "${INV_BEFORE}" ] || fail "inventory not restored"
ok "compensation complete — order COMPENSATED, inventory fully refunded"
pause 2

# ── Phase 3: where to look next ──────────────────────────────────────────────
banner "Phase 3 · Where to look"
cat <<EOF
    ${BOLD}Saga state machine${RESET}
      curl -s ${ORDER_URL}/admin/sagas | jq .

    ${BOLD}Grafana — Saga Overview dashboard${RESET}
      open http://localhost:3000/d/dop-saga-overview/saga-overview

    ${BOLD}RabbitMQ management UI${RESET} (guest/guest)
      open http://localhost:15672

    ${BOLD}Prometheus targets${RESET}
      open http://localhost:9090/targets
EOF
