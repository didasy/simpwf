package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
	"github.com/simpwf/workflow-engine/pkg/ids"
)

// NodeDefinitionHandler serves /v1/node/definition routes.
type NodeDefinitionHandler struct {
	svc service.NodeDefinitionService
}

// NewNodeDefinitionHandler builds the handler.
func NewNodeDefinitionHandler(svc service.NodeDefinitionService) *NodeDefinitionHandler {
	return &NodeDefinitionHandler{svc: svc}
}

// Create handles POST /v1/node/definition.
//
// @Summary Create node definition
// @Tags node-definitions
// @Accept json
// @Produce json
// @Param request body CreateNodeDefinitionRequest true "Node definition"
// @Success 201 {object} NodeDefinitionResponse
// @Failure 400,409,422,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/node/definition [post]
func (h *NodeDefinitionHandler) Create(c *gin.Context) {
	var req CreateNodeDefinitionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		WriteProblem(c, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.Name == "" || req.Type == "" || req.Content == nil {
		WriteProblem(c, http.StatusUnprocessableEntity, "name, type, and content are required")
		return
	}
	def, err := h.svc.Create(c.Request.Context(), service.CreateNodeDefinition{
		Name:              req.Name,
		Type:              req.Type,
		PreviousVersionID: req.PreviousVersionID,
		Content:           req.Content,
	})
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusCreated, toNodeDefinitionResponse(def))
}

// List handles GET /v1/node/definition.
//
// @Summary List node definitions
// @Tags node-definitions
// @Produce json
// @Param page query int false "Page number"
// @Param per_page query int false "Items per page"
// @Param order query string false "Sort order"
// @Param ids query []string false "Definition IDs"
// @Param name query string false "Name filter"
// @Param lineage_id query string false "Lineage ID"
// @Param version query int false "Version"
// @Param latest_only query bool false "Only latest versions"
// @Param type query string false "Node type"
// @Success 200 {object} ListResponse[NodeDefinitionResponse]
// @Failure 400,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/node/definition [get]
func (h *NodeDefinitionHandler) List(c *gin.Context) {
	q, err := ParseListQuery(c, ListKindNode)
	if err != nil {
		WriteProblem(c, http.StatusBadRequest, err.Error())
		return
	}
	items, total, err := h.svc.List(c.Request.Context(), toRepositoryQuery(q))
	if err != nil {
		WriteError(c, err)
		return
	}
	resp := make([]NodeDefinitionResponse, 0, len(items))
	for _, def := range items {
		resp = append(resp, toNodeDefinitionResponse(def))
	}
	c.JSON(http.StatusOK, ListResponse[NodeDefinitionResponse]{
		Items:      resp,
		Page:       q.Page,
		PerPage:    q.PerPage,
		Total:      total,
		TotalPages: totalPages(total, q.PerPage),
	})
}

// Get handles GET /v1/node/definition/{id}.
//
// @Summary Get node definition
// @Tags node-definitions
// @Produce json
// @Param id path string true "Definition ID"
// @Success 200 {object} NodeDefinitionResponse
// @Failure 400,404,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/node/definition/{id} [get]
func (h *NodeDefinitionHandler) Get(c *gin.Context) {
	id := c.Param("id")
	if !ids.Valid(id) {
		WriteProblem(c, http.StatusBadRequest, "id must be a valid uuid")
		return
	}
	def, err := h.svc.Get(c.Request.Context(), id)
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusOK, toNodeDefinitionResponse(def))
}

// Delete handles DELETE /v1/node/definition/{id}.
//
// @Summary Delete node definition
// @Tags node-definitions
// @Param id path string true "Definition ID"
// @Success 204
// @Failure 400,404,409,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/node/definition/{id} [delete]
func (h *NodeDefinitionHandler) Delete(c *gin.Context) {
	id := c.Param("id")
	if !ids.Valid(id) {
		WriteProblem(c, http.StatusBadRequest, "id must be a valid uuid")
		return
	}
	if err := h.svc.Delete(c.Request.Context(), id); err != nil {
		WriteError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func toNodeDefinitionResponse(def model.NodeDefinition) NodeDefinitionResponse {
	return NodeDefinitionResponse{
		ID:                def.ID,
		Name:              def.Name,
		Version:           def.Version,
		PreviousVersionID: def.PreviousVersionID,
		LineageID:         def.LineageID,
		Type:              def.Type,
		Content:           def.Content,
		CreatedBy:         def.CreatedBy,
		UpdatedBy:         def.UpdatedBy,
		CreatedAt:         def.CreatedAt,
		UpdatedAt:         def.UpdatedAt,
	}
}

// toRepositoryQuery converts a parsed list query into the repository query.
func toRepositoryQuery(q *ListQuery) repository.DefinitionListQuery {
	return repository.DefinitionListQuery{
		Page:       q.Page,
		PerPage:    q.PerPage,
		Order:      q.Order,
		IDs:        q.IDs,
		Name:       q.Name,
		LineageID:  q.LineageID,
		Version:    q.Version,
		LatestOnly: q.LatestOnly,
		Type:       q.Type,
	}
}

func totalPages(total int64, perPage int) int64 {
	if perPage <= 0 {
		return 0
	}
	return (total + int64(perPage) - 1) / int64(perPage)
}
