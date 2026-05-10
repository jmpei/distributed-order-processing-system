#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "${SCRIPT_DIR}/.." && pwd )"
COMPOSE_DIR="${PROJECT_ROOT}/deploy"

ORDER_URL="${ORDER_URL:-http://localhost:8081}"
INVENTORY_URL="${INVENTORY_URL:-http://localhost:8082}"
PRODUCT_ID="${PRODUCT_ID:-2003}"
USER_ID="${USER_ID:-44}"
STOCK=10
QUANTITY=1
DOWN_WAIT=5
RECOVERY_WAIT=15

echo "=== test-crash-recovery ==="

restart_inventory() {
  ( cd "${COMPOSE_DIR}" && docker compose start inventory-service > /dev/null ) || true
}

echo "[1] seeding product ${PRODUCT_ID} qty=${STOCK} (inventory-service still up)..."
curl -s -o /dev/null -X POST "${INVENTORY_URL}/inventory" \
  -H 'Content-Type: application/json' \
  -d "{\"product_id\":${PRODUCT_ID},\"available_qty\":${STOCK}}" || true
curl -sf -o /dev/null -X PUT "${INVENTORY_URL}/inventory/${PRODUCT_ID}" \
  -H 'Content-Type: application/json' \
  -d "{\"available_qty\":${STOCK}}"

echo "[2] stopping inventory-service..."
( cd "${COMPOSE_DIR}" && docker compose stop inventory-service > /dev/null )

echo "[3] placing order qty=${QUANTITY} while inventory-service is down..."
ORDER_RESP=$(curl -sf -X POST "${ORDER_URL}/orders" \
  -H 'Content-Type: application/json' \
  -d "{\"user_id\":${USER_ID},\"product_id\":${PRODUCT_ID},\"quantity\":${QUANTITY},\"total_amount\":25.00}")
ORDER_ID=$(echo "${ORDER_RESP}" | jq -r .order_id)
echo "    order_id=${ORDER_ID}"

echo "[4] waiting ${DOWN_WAIT}s, expecting order to remain PENDING..."
sleep "${DOWN_WAIT}"
STATUS=$(curl -sf "${ORDER_URL}/orders/${ORDER_ID}" | jq -r .status)
echo "    order status=${STATUS}"
if [ "${STATUS}" != "PENDING" ]; then
  echo "FAIL: expected PENDING while inventory is down, got ${STATUS}"
  restart_inventory
  exit 1
fi

echo "[5] starting inventory-service..."
restart_inventory

echo "[6] waiting ${RECOVERY_WAIT}s for saga to complete..."
sleep "${RECOVERY_WAIT}"

STATUS=$(curl -sf "${ORDER_URL}/orders/${ORDER_ID}" | jq -r .status)
echo "    order status=${STATUS}"
if [ "${STATUS}" = "CONFIRMED" ]; then
  echo "PASS: order ${ORDER_ID} recovered to CONFIRMED after inventory restart"
  exit 0
fi

echo "FAIL: expected CONFIRMED after recovery, got ${STATUS}"
exit 1
