package handler

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

// SecretHandler serves /v1/secrets routes. Secret values are masked before
// every response; plaintext is only available to the internal execution path.
type SecretHandler struct {
	svc service.SecretService
}

// NewSecretHandler builds the handler.
func NewSecretHandler(svc service.SecretService) *SecretHandler {
	return &SecretHandler{svc: svc}
}

// Create handles POST /v1/secrets.
//
// @Summary Create secret
// @Tags secrets
// @Accept json
// @Produce json
// @Param request body CreateSecretRequest true "Secret"
// @Success 201 {object} SecretResponse
// @Failure 400,409,422,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/secrets [post]
func (h *SecretHandler) Create(c *gin.Context) {
	var req CreateSecretRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		WriteProblem(c, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.Key == "" || req.Value == "" {
		WriteProblem(c, http.StatusUnprocessableEntity, "key and value are required")
		return
	}
	secret, err := h.svc.Create(c.Request.Context(), req.Key, req.Value)
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusCreated, toSecretResponse(secret))
}

// List handles GET /v1/secrets.
//
// @Summary List secrets
// @Tags secrets
// @Produce json
// @Param page query int false "Page number"
// @Param per_page query int false "Items per page"
// @Success 200 {object} ListResponse[SecretResponse]
// @Failure 400,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/secrets [get]
func (h *SecretHandler) List(c *gin.Context) {
	page, perPage, err := parsePagination(c)
	if err != nil {
		WriteProblem(c, http.StatusBadRequest, err.Error())
		return
	}
	secrets, total, err := h.svc.List(c.Request.Context(), page, perPage)
	if err != nil {
		WriteError(c, err)
		return
	}
	items := make([]SecretResponse, 0, len(secrets))
	for _, secret := range secrets {
		items = append(items, toSecretResponse(secret))
	}
	c.JSON(http.StatusOK, ListResponse[SecretResponse]{
		Items:      items,
		Page:       page,
		PerPage:    perPage,
		Total:      total,
		TotalPages: totalPages(total, perPage),
	})
}

// Get handles GET /v1/secrets/{key}.
//
// @Summary Get secret
// @Tags secrets
// @Produce json
// @Param key path string true "Secret key"
// @Success 200 {object} SecretResponse
// @Failure 400,404,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/secrets/{key} [get]
func (h *SecretHandler) Get(c *gin.Context) {
	key := c.Param("key")
	secret, err := h.svc.Get(c.Request.Context(), key)
	if err != nil {
		writeSecretPathError(c, err)
		return
	}
	c.JSON(http.StatusOK, toSecretResponse(secret))
}

// Delete handles DELETE /v1/secrets/{key}.
//
// @Summary Delete secret
// @Tags secrets
// @Param key path string true "Secret key"
// @Success 204
// @Failure 400,404,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/secrets/{key} [delete]
func (h *SecretHandler) Delete(c *gin.Context) {
	key := c.Param("key")
	if err := h.svc.Delete(c.Request.Context(), key); err != nil {
		writeSecretPathError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func writeSecretPathError(c *gin.Context, err error) {
	if errors.Is(err, model.ErrInvalid) {
		WriteProblem(c, http.StatusBadRequest, err.Error())
		return
	}
	WriteError(c, err)
}

func toSecretResponse(secret repository.Secret) SecretResponse {
	return SecretResponse{
		Key:         secret.Key,
		ValueMasked: service.SecretMask,
		CreatedAt:   secret.CreatedAt,
		UpdatedAt:   secret.UpdatedAt,
	}
}
