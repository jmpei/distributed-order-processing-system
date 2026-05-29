//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMain compiles all three service binaries once, then hands control to the suite.
// Compiling here (instead of per-test) saves ~30s in cold caches and 5-10s in warm.
func TestMain(m *testing.M) {
	root, err := findProjectRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "find project root: %v\n", err)
		os.Exit(1)
	}
	projectRoot = root

	dir, err := os.MkdirTemp("", "integration-bin-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "tempdir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	build := func(name string) (string, error) {
		out := filepath.Join(dir, name)
		cmd := exec.Command("go", "build", "-o", out, "./cmd")
		cmd.Dir = filepath.Join(root, "services", name)
		if output, err := cmd.CombinedOutput(); err != nil {
			return "", fmt.Errorf("build %s: %v\n%s", name, err, string(output))
		}
		return out, nil
	}

	for _, name := range []string{"order-service", "inventory-service", "payment-service"} {
		bin, err := build(name)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		switch name {
		case "order-service":
			binaries.order = bin
		case "inventory-service":
			binaries.inventory = bin
		case "payment-service":
			binaries.payment = bin
		}
	}

	os.Exit(m.Run())
}

func findProjectRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	cur := wd
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(filepath.Join(cur, "deploy", "docker-compose.yml")); err == nil {
			return cur, nil
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			break
		}
		cur = parent
	}
	return "", fmt.Errorf("project root not found from %s", wd)
}

// ─── TestE2E_PaymentFailure_CompensatesInventory ────────────────────────────

// Configures payment-service to fail 100% of the time; verifies the saga walks
// through compensation and the inventory is fully refunded.
func TestE2E_PaymentFailure_CompensatesInventory(t *testing.T) {
	infra := setupInfra(t)
	infra.resetSchemas(t)

	orderSvc, _, _ := infra.startAllServices(t, 1.0)

	const productID = 8001
	const initialQty = 10
	const orderQty = 3
	seedInventory(t, infra, productID, initialQty)

	// Place an order. With 100% payment failure, the saga should compensate.
	orderID := postOrder(t, orderSvc.port, productID, orderQty, 99.99)

	require.Eventually(t, func() bool {
		status := getOrderStatus(t, orderSvc.port, orderID)
		return status == "COMPENSATED" || status == "FAILED"
	}, 30*time.Second, 200*time.Millisecond, "order never reached terminal compensation state")

	// Order ends in COMPENSATED (payment failed, inventory was released).
	assert.Equal(t, "COMPENSATED", getOrderStatus(t, orderSvc.port, orderID))

	// Inventory should be fully refunded: available back to 10, reserved=0.
	avail, reserved := getInventory(t, infra, productID)
	assert.Equal(t, initialQty, avail, "available_qty should be restored after compensation")
	assert.Equal(t, 0, reserved, "reserved_qty should be 0 after compensation")

	// saga_state should be COMPENSATED.
	sagaStatus := getSagaStatusForOrder(t, infra, orderID)
	assert.Equal(t, "COMPENSATED", sagaStatus, "saga_state status should be COMPENSATED")
}

// ─── TestE2E_ConcurrentOrders_NoOversell ────────────────────────────────────

// Fires N concurrent orders against a stock of M (M < N). The system must
// confirm exactly M orders and fail the rest with no negative inventory.
func TestE2E_ConcurrentOrders_NoOversell(t *testing.T) {
	infra := setupInfra(t)
	infra.resetSchemas(t)

	orderSvc, _, _ := infra.startAllServices(t, 0.0)

	const productID = 8002
	const stockQty = 10
	const concurrentOrders = 50
	seedInventory(t, infra, productID, stockQty)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		ids     = make([]uint64, 0, concurrentOrders)
		started int32
	)

	for i := 0; i < concurrentOrders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			atomic.AddInt32(&started, 1)
			id, err := postOrderAny(t, orderSvc.port, productID, 1, 10.0)
			if err != nil {
				return
			}
			mu.Lock()
			ids = append(ids, id)
			mu.Unlock()
		}()
	}
	wg.Wait()
	require.Equal(t, int32(concurrentOrders), atomic.LoadInt32(&started))
	require.Len(t, ids, concurrentOrders, "every POST /orders should return an id")

	// Wait for every saga to settle.
	require.Eventually(t, func() bool {
		for _, id := range ids {
			s := getOrderStatus(t, orderSvc.port, id)
			if s != "CONFIRMED" && s != "FAILED" && s != "COMPENSATED" {
				return false
			}
		}
		return true
	}, 60*time.Second, 500*time.Millisecond, "not all sagas settled")

	confirmed, failed := 0, 0
	for _, id := range ids {
		switch getOrderStatus(t, orderSvc.port, id) {
		case "CONFIRMED":
			confirmed++
		case "FAILED", "COMPENSATED":
			failed++
		}
	}

	avail, reserved := getInventory(t, infra, productID)
	assert.Equal(t, 0, avail, "available_qty should be exactly 0, not negative")
	assert.GreaterOrEqual(t, reserved, 0, "reserved_qty should never go negative")

	// At most stockQty orders confirmed. The exact count may be less if any
	// optimistic-lock retries got starved, but it must never exceed stock.
	assert.LessOrEqual(t, confirmed, stockQty, "should never confirm more than stock")
	assert.Equal(t, stockQty, confirmed, "expected exactly %d confirmed orders", stockQty)
	assert.Equal(t, concurrentOrders-stockQty, failed, "expected %d failed orders", concurrentOrders-stockQty)
}

// ─── TestE2E_RetryTopologyDeclared_HappyPathCompletes ───────────────────────

// Asserts the delayed-retry topology is actually declared on the broker by the
// services at startup (the <queue>.retry queues), and that a normal order still
// completes end-to-end with that topology in place — i.e. the retry plumbing is
// wired and non-regressive.
func TestE2E_RetryTopologyDeclared_HappyPathCompletes(t *testing.T) {
	infra := setupInfra(t)
	infra.resetSchemas(t)

	orderSvc, _, _ := infra.startAllServices(t, 0.0)

	// Each retry queue must exist. A passive declare fails (and closes the
	// channel) if the queue is absent, so use a fresh channel per check.
	conn, err := amqp.Dial(infra.amqpURL)
	require.NoError(t, err)
	defer conn.Close()
	for _, q := range []string{"inventory.commands.retry", "payment.commands.retry", "order.events.retry"} {
		ch, err := conn.Channel()
		require.NoError(t, err)
		_, err = ch.QueueDeclarePassive(q, true, false, false, false, nil)
		require.NoError(t, err, "retry queue %s must be declared by its service at startup", q)
		_ = ch.Close()
	}

	const productID = 8003
	const initialQty = 10
	const orderQty = 2
	seedInventory(t, infra, productID, initialQty)

	orderID := postOrder(t, orderSvc.port, productID, orderQty, 199.98)

	require.Eventually(t, func() bool {
		return getOrderStatus(t, orderSvc.port, orderID) == "CONFIRMED"
	}, 30*time.Second, 200*time.Millisecond, "happy-path order never reached CONFIRMED")

	avail, reserved := getInventory(t, infra, productID)
	assert.Equal(t, initialQty-orderQty, avail, "available_qty should be decremented by the order qty")
	assert.Equal(t, orderQty, reserved, "reserved_qty should equal the order qty")
	assert.Equal(t, "COMPLETED", getSagaStatusForOrder(t, infra, orderID), "saga_state should be COMPLETED")
}

// ─── helpers used by the tests above ───────────────────────────────────────

func seedInventory(t *testing.T, infra *testInfra, productID uint64, qty int) {
	t.Helper()
	db := infra.schemaDB(t, "inventory_db")
	defer db.Close()
	// Wait until inventory-service has run AutoMigrate so the inventories table exists.
	require.Eventually(t, func() bool {
		var n int
		err := db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='inventory_db' AND table_name='inventories'").Scan(&n)
		return err == nil && n == 1
	}, 15*time.Second, 200*time.Millisecond, "inventories table never appeared")

	now := time.Now().UTC()
	_, err := db.Exec(`INSERT INTO inventories (product_id, available_qty, reserved_qty, version, created_at, updated_at)
		VALUES (?, ?, 0, 0, ?, ?)
		ON DUPLICATE KEY UPDATE available_qty = VALUES(available_qty), reserved_qty = 0, version = 0`,
		productID, qty, now, now)
	require.NoError(t, err)
}

func postOrder(t *testing.T, port int, productID uint64, qty int, amount float64) uint64 {
	t.Helper()
	id, err := postOrderAny(t, port, productID, qty, amount)
	require.NoError(t, err)
	return id
}

func postOrderAny(t *testing.T, port int, productID uint64, qty int, amount float64) (uint64, error) {
	t.Helper()
	body := map[string]any{
		"user_id":      1,
		"product_id":   productID,
		"quantity":     qty,
		"total_amount": amount,
	}
	b, _ := json.Marshal(body)
	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/orders", port),
		"application/json", bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		buf, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("POST /orders status=%d body=%s", resp.StatusCode, string(buf))
	}
	var out struct {
		OrderID uint64 `json:"order_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.OrderID, nil
}

func getOrderStatus(t *testing.T, port int, orderID uint64) string {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/orders/%d", port, orderID))
	require.NoError(t, err)
	defer resp.Body.Close()
	var out struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return strings.ToUpper(out.Status)
}

func getInventory(t *testing.T, infra *testInfra, productID uint64) (avail, reserved int) {
	t.Helper()
	db := infra.schemaDB(t, "inventory_db")
	defer db.Close()
	require.NoError(t, db.QueryRow(
		"SELECT available_qty, reserved_qty FROM inventories WHERE product_id = ?", productID,
	).Scan(&avail, &reserved))
	return
}

func getSagaStatusForOrder(t *testing.T, infra *testInfra, orderID uint64) string {
	t.Helper()
	db := infra.schemaDB(t, "orders_db")
	defer db.Close()
	var status string
	require.NoError(t, db.QueryRow(
		"SELECT status FROM saga_states WHERE order_id = ?", orderID,
	).Scan(&status))
	return status
}

