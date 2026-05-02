package handler

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/model"
	"github.com/TomatoesSuck/distributed-order-processing/order-service/internal/service"
)

type OrderHandler struct {
	svc          *service.OrderService
	orchestrator *service.SagaOrchestrator
}

func NewOrderHandler(svc *service.OrderService, orchestrator *service.SagaOrchestrator) *OrderHandler {
	return &OrderHandler{svc: svc, orchestrator: orchestrator}
}

func (h *OrderHandler) Register(r *gin.Engine) {
	r.POST("/orders", h.Create)
	r.GET("/orders/:id", h.Get)
	r.GET("/orders", h.List)
}

type createOrderRequest struct {
	UserID      uint64  `json:"user_id"      binding:"required"`
	ProductID   uint64  `json:"product_id"   binding:"required"`
	Quantity    int     `json:"quantity"     binding:"required,min=1"`
	TotalAmount float64 `json:"total_amount" binding:"required,gt=0"`
}

func (h *OrderHandler) Create(c *gin.Context) {
	var req createOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error(), "code": "INVALID_REQUEST"})
		return
	}

	order := &model.Order{
		UserID:      req.UserID,
		ProductID:   req.ProductID,
		Quantity:    req.Quantity,
		TotalAmount: req.TotalAmount,
	}

	if err := h.svc.CreateOrder(c.Request.Context(), order); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "code": "CREATE_FAILED"})
		return
	}

	if err := h.orchestrator.StartSaga(c.Request.Context(), order); err != nil {
		// Order is persisted; saga start failure is logged and surfaced but order_id is still returned.
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "code": "SAGA_START_FAILED"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"order_id": order.ID, "status": order.Status})
}

func (h *OrderHandler) Get(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id", "code": "INVALID_ID"})
		return
	}

	order, err := h.svc.GetOrder(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "order not found", "code": "NOT_FOUND"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "code": "GET_FAILED"})
		return
	}

	c.JSON(http.StatusOK, order)
}

func (h *OrderHandler) List(c *gin.Context) {
	userIDStr := c.Query("user_id")
	if userIDStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id is required", "code": "MISSING_USER_ID"})
		return
	}

	userID, err := strconv.ParseUint(userIDStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid user_id", "code": "INVALID_USER_ID"})
		return
	}

	orders, err := h.svc.ListOrders(c.Request.Context(), userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "code": "LIST_FAILED"})
		return
	}

	c.JSON(http.StatusOK, orders)
}
