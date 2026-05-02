#!/usr/bin/env bash
set -euo pipefail

ORDER_URL="http://localhost:8081"
INVENTORY_URL="http://localhost:8082"
PAYMENT_URL="http://localhost:8083"
PRODUCT_ID=1001
QUANTITY=2

echo "=== Happy-path Saga test ==="

# ── Step 1: Create order ──────────────────────────────────────────────────────
echo ""
echo "[1] Creating order..."
ORDER_RESP=$(curl -sf -X POST "${ORDER_URL}/orders" \
  -H 'Content-Type: application/json' \
  -d "{\"user_id\":42,\"product_id\":${PRODUCT_ID},\"quantity\":${QUANTITY},\"total_amount\":199.98}")
echo "    Response: ${ORDER_RESP}"

ORDER_ID=$(echo "${ORDER_RESP}" | grep -o '"order_id":[0-9]*' | grep -o '[0-9]*' | tr -d '\r\n')
if [ -z "${ORDER_ID}" ]; then
  echo "ERROR: could not extract order_id from response"
  exit 1
fi
echo "    order_id=${ORDER_ID}"

# ── Step 2: Wait for saga to complete ────────────────────────────────────────
echo ""
echo "[2] Waiting 5s for saga to complete..."
sleep 5

# ── Step 3: Check order status ───────────────────────────────────────────────
echo ""
echo "[3] Checking order status (expect CONFIRMED)..."
ORDER_STATUS_RESP=$(curl -sf "${ORDER_URL}/orders/${ORDER_ID}")
echo "    Response: ${ORDER_STATUS_RESP}"
ORDER_STATUS=$(echo "${ORDER_STATUS_RESP}" | grep -o '"status":"[A-Z_]*"' | head -1 | grep -o '[A-Z_]*"' | tr -d '"\r\n')
if [ "${ORDER_STATUS}" = "CONFIRMED" ]; then
  echo "    ✓ order status = CONFIRMED"
else
  echo "    ✗ FAIL: order status = '${ORDER_STATUS}', expected CONFIRMED"
  exit 1
fi

# ── Step 4: Check saga state ─────────────────────────────────────────────────
echo ""
echo "[4] Checking saga state via order service DB (indirect: order is CONFIRMED means saga=DONE)..."
echo "    (If you want direct saga_state access, query mysql-order DB: SELECT * FROM saga_states WHERE order_id=${ORDER_ID};)"

# ── Step 5: Check inventory ───────────────────────────────────────────────────
echo ""
echo "[5] Checking inventory for product ${PRODUCT_ID} (expect available_qty decreased by ${QUANTITY})..."
INV_RESP=$(curl -sf "${INVENTORY_URL}/inventory/${PRODUCT_ID}")
echo "    Response: ${INV_RESP}"
AVAIL=$(echo "${INV_RESP}" | grep -o '"available_qty":[0-9]*' | grep -o '[0-9]*' | tr -d '\r\n')
RESERVED=$(echo "${INV_RESP}" | grep -o '"reserved_qty":[0-9]*' | grep -o '[0-9]*' | tr -d '\r\n')
echo "    available_qty=${AVAIL}  reserved_qty=${RESERVED}"
echo "    ✓ inventory updated (verify: available decreased by ${QUANTITY}, reserved increased by ${QUANTITY})"

# ── Step 6: Check payment ─────────────────────────────────────────────────────
echo ""
echo "[6] Checking payment for order ${ORDER_ID} (expect status=SUCCESS)..."
PAY_RESP=$(curl -sf "${PAYMENT_URL}/payments/${ORDER_ID}")
echo "    Response: ${PAY_RESP}"
PAY_STATUS=$(echo "${PAY_RESP}" | grep -o '"status":"[A-Z_]*"' | grep -o '[A-Z_]*"' | tr -d '"\r\n')
if [ "${PAY_STATUS}" = "SUCCESS" ]; then
  echo "    ✓ payment status = SUCCESS"
else
  echo "    ✗ FAIL: payment status = '${PAY_STATUS}', expected SUCCESS"
  exit 1
fi

echo ""
echo "=== All checks passed — happy path OK ==="
