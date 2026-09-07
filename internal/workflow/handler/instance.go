package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

// InstanceHandler serves workflow instance routes.
type InstanceHandler struct {
	svc service.InstanceService
}

// NewInstanceHandler builds the handler.
func NewInstanceHandler(svc service.InstanceService) *InstanceHandler {
	return &InstanceHandler{svc: svc}
}

// Create handles POST /v1/workflow/instance.
//
// @Summary Create workflow instance
// @Tags workflow-instances
// @Accept json
// @Produce json
// @Param request body CreateInstanceRequest true "Workflow instance"
// @Success 202 {object} CreateInstanceResponse
// @Failure 400,404,409,422,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/workflow/instance [post]
func (h *InstanceHandler) Create(c *gin.Context) {
	var req CreateInstanceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		WriteProblem(c, http.StatusBadRequest, "invalid request body")
		return
	}
	inst, err := h.svc.Create(c.Request.Context(), service.CreateInstance{
		WorkflowDefinitionID: req.WorkflowDefinitionID,
		Context:              req.Context,
	})
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusAccepted, CreateInstanceResponse{ID: inst.ID, Status: string(inst.Status)})
}

// List handles GET /v1/workflow/instance.
//
// @Summary List workflow instances
// @Tags workflow-instances
// @Produce json
// @Param page query int false "Page number"
// @Param per_page query int false "Items per page"
// @Param order query string false "Sort order"
// @Param id query []string false "Instance IDs"
// @Param workflow_definition_id query string false "Workflow definition ID"
// @Param status query []string false "Statuses"
// @Success 200 {object} ListResponse[InstanceSummaryResponse]
// @Failure 400,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/workflow/instance [get]
func (h *InstanceHandler) List(c *gin.Context) {
	q, err := ParseInstanceListQuery(c)
	if err != nil {
		WriteProblem(c, http.StatusBadRequest, err.Error())
		return
	}
	items, total, err := h.svc.List(c.Request.Context(), toInstanceListQuery(q))
	if err != nil {
		WriteError(c, err)
		return
	}
	resp := make([]InstanceSummaryResponse, 0, len(items))
	for _, inst := range items {
		resp = append(resp, toInstanceSummaryResponse(inst))
	}
	c.JSON(http.StatusOK, ListResponse[InstanceSummaryResponse]{
		Items:      resp,
		Page:       q.Page,
		PerPage:    q.PerPage,
		Total:      total,
		TotalPages: totalPages(total, q.PerPage),
	})
}

// Status handles GET /v1/workflow/instance/{id}/status.
//
// @Summary Get workflow instance status
// @Tags workflow-instances
// @Produce json
// @Param id path string true "Instance ID"
// @Success 200 {object} InstanceStatusResponse
// @Failure 404,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/workflow/instance/{id}/status [get]
func (h *InstanceHandler) Status(c *gin.Context) {
	d, err := h.svc.GetStatusDetail(c.Request.Context(), c.Param("id"))
	if err != nil {
		WriteError(c, err)
		return
	}
	inst := d.Instance
	waitingReason := nullableString(string(inst.WaitingReason))
	errorMsg := nullableString(inst.Error)
	resp := InstanceStatusResponse{
		ID:                    inst.ID,
		WorkflowDefinitionID:  inst.WorkflowDefinitionID,
		Status:                string(inst.Status),
		WaitingReason:         waitingReason,
		PauseRequested:        inst.PauseRequested,
		TerminationPending:    inst.TerminationPending,
		CurrentGroupID:        nullableString(inst.CurrentGroupID),
		CurrentNodeID:         nullableString(inst.CurrentNodeID),
		CurrentNodeInstanceID: nodeInstanceIDString(inst.ID, d.CurrentNodeInstanceID),
		Attempt:               d.Attempt,
		Counters:              inst.Counters,
		Nodes:                 toNodeOccurrenceResponses(d.Nodes),
		Error:                 errorMsg,
		StartedAt:             inst.StartedAt,
		FinishedAt:            inst.FinishedAt,
		CreatedBy:             inst.CreatedBy,
		UpdatedBy:             inst.UpdatedBy,
		CreatedAt:             inst.CreatedAt,
		UpdatedAt:             inst.UpdatedAt,
	}
	c.JSON(http.StatusOK, resp)
}

// Context handles GET /v1/workflow/instance/{id}/context.
//
// @Summary Get workflow instance context
// @Tags workflow-instances
// @Produce json
// @Param id path string true "Instance ID"
// @Success 200 {object} InstanceContextResponse
// @Failure 404,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/workflow/instance/{id}/context [get]
func (h *InstanceHandler) Context(c *gin.Context) {
	inst, err := h.svc.GetContext(c.Request.Context(), c.Param("id"))
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusOK, InstanceContextResponse{ID: inst.ID, Context: inst.Context})
}

// UpdateContext handles PUT /v1/workflow/instance/{id}/context.
//
// @Summary Replace workflow instance context
// @Tags workflow-instances
// @Accept json
// @Produce json
// @Param id path string true "Instance ID"
// @Param X-Context-Update-Reason header string false "Audit reason"
// @Param request body object true "Full replacement context"
// @Success 200 {object} InstanceContextResponse
// @Failure 400,404,409,422,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/workflow/instance/{id}/context [put]
func (h *InstanceHandler) UpdateContext(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil || len(body) == 0 || !json.Valid(body) {
		WriteProblem(c, http.StatusBadRequest, "invalid request body")
		return
	}
	inst, err := h.svc.UpdateContext(c.Request.Context(), service.UpdateContext{
		InstanceID: c.Param("id"),
		Context:    body,
		Reason:     c.GetHeader("X-Context-Update-Reason"),
	})
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusOK, InstanceContextResponse{ID: inst.ID, Context: inst.Context})
}

// NodeDebug handles GET /v1/workflow/instance/{id}/status/node/{node_id}.
//
// @Summary Get workflow node debug detail
// @Tags workflow-instances
// @Produce json
// @Param id path string true "Instance ID"
// @Param node_id path string true "Node occurrence ID"
// @Param attempt query int false "Attempt number"
// @Success 200 {object} NodeDebugResponse
// @Failure 400,404,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/workflow/instance/{id}/status/node/{node_id} [get]
func (h *InstanceHandler) NodeDebug(c *gin.Context) {
	attempt := 0
	if v := c.Query("attempt"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			WriteProblem(c, http.StatusBadRequest, "query: attempt must be a positive integer")
			return
		}
		attempt = n
	}
	d, err := h.svc.NodeDebug(c.Request.Context(), c.Param("id"), c.Param("node_id"), attempt)
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusOK, toNodeDebugResponse(d))
}

// Input handles PUT /v1/workflow/instance/{id}/input.
//
// @Summary Deliver input to workflow instance
// @Tags workflow-instances
// @Accept json
// @Produce json
// @Param id path string true "Instance ID"
// @Param Idempotency-Key header string true "Idempotency key"
// @Param request body object true "Input payload"
// @Success 202 {object} InputDeliveryResponse
// @Failure 400,404,409,422,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/workflow/instance/{id}/input [put]
func (h *InstanceHandler) Input(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		WriteProblem(c, http.StatusBadRequest, "cannot read request body")
		return
	}
	delivery, err := h.svc.DeliverInput(c.Request.Context(), service.DeliverInput{
		InstanceID:     c.Param("id"),
		IdempotencyKey: c.GetHeader("Idempotency-Key"),
		Payload:        body,
		Source:         model.InputChannelHTTP,
	})
	if err != nil {
		WriteError(c, err)
		return
	}
	if !delivery.Accepted {
		WriteProblem(c, http.StatusUnprocessableEntity, delivery.Error)
		return
	}
	c.JSON(http.StatusAccepted, InputDeliveryResponse{Accepted: true})
}

// Pause handles POST /v1/workflow/instance/{id}/pause.
//
// @Summary Pause workflow instance
// @Tags workflow-instances
// @Produce json
// @Param id path string true "Instance ID"
// @Success 200 {object} PauseResponse
// @Success 202 {object} PauseResponse
// @Failure 404,409,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/workflow/instance/{id}/pause [post]
func (h *InstanceHandler) Pause(c *gin.Context) {
	res, err := h.svc.Pause(c.Request.Context(), c.Param("id"))
	if err != nil {
		WriteError(c, err)
		return
	}
	status := http.StatusOK
	if res.PauseRequested {
		status = http.StatusAccepted
	}
	c.JSON(status, PauseResponse{Status: string(res.Status), PauseRequested: res.PauseRequested})
}

// Resume handles POST /v1/workflow/instance/{id}/resume.
//
// @Summary Resume workflow instance
// @Tags workflow-instances
// @Produce json
// @Param id path string true "Instance ID"
// @Success 200 {object} ResumeResponse
// @Failure 404,409,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/workflow/instance/{id}/resume [post]
func (h *InstanceHandler) Resume(c *gin.Context) {
	res, err := h.svc.Resume(c.Request.Context(), c.Param("id"))
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusOK, ResumeResponse{Status: string(res.Status)})
}

// Stop handles POST /v1/workflow/instance/{id}/stop.
//
// @Summary Stop workflow instance
// @Tags workflow-instances
// @Produce json
// @Param id path string true "Instance ID"
// @Success 200 {object} StopResponse
// @Failure 404,409,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/workflow/instance/{id}/stop [post]
func (h *InstanceHandler) Stop(c *gin.Context) {
	res, err := h.svc.Stop(c.Request.Context(), c.Param("id"), "operator")
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusOK, StopResponse{Status: string(res.Status), TerminationPending: res.TerminationPending})
}

// Rollback handles POST /v1/workflow/instance/{id}/rollback.
//
// @Summary Roll back a paused or failed instance to a prior node occurrence
// @Tags workflow-instances
// @Accept json
// @Produce json
// @Param id path string true "Instance ID"
// @Param request body RollbackRequest true "Rollback target"
// @Success 200 {object} RollbackResponse
// @Failure 400,404,409,422,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/workflow/instance/{id}/rollback [post]
func (h *InstanceHandler) Rollback(c *gin.Context) {
	var req RollbackRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.TargetOccurrenceID == "" {
		WriteProblem(c, http.StatusBadRequest, "invalid request body")
		return
	}
	res, err := h.svc.Rollback(c.Request.Context(), service.RollbackRequest{
		InstanceID:         c.Param("id"),
		TargetOccurrenceID: req.TargetOccurrenceID,
		Reason:             req.Reason,
	})
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusOK, RollbackResponse{Status: string(res.Status), CurrentNodeID: res.CurrentNodeID})
}

// toNodeOccurrenceResponses maps the service nodes view onto the openapi
// NodeOccurrence schema. A nil map stays nil so the field is omitted.
func toNodeOccurrenceResponses(nodes map[string]service.NodeOccurrence) map[string]NodeOccurrenceResponse {
	if nodes == nil {
		return nil
	}
	out := make(map[string]NodeOccurrenceResponse, len(nodes))
	for id, e := range nodes {
		out[id] = NodeOccurrenceResponse{
			OccurrenceID: e.OccurrenceID,
			Status:       e.Status,
			Attempt:      e.Attempt,
			Rollbackable: e.Rollbackable,
		}
	}
	return out
}

// toInstanceListQuery converts a parsed list query into the repository query.
func toInstanceListQuery(q *InstanceListQuery) repository.InstanceListQuery {
	return repository.InstanceListQuery{
		Page:                 q.Page,
		PerPage:              q.PerPage,
		Order:                q.Order,
		IDs:                  q.IDs,
		WorkflowDefinitionID: q.WorkflowDefinitionID,
		Statuses:             q.Statuses,
	}
}

// toInstanceSummaryResponse maps an instance onto the compact list DTO.
func toInstanceSummaryResponse(inst model.WorkflowInstance) InstanceSummaryResponse {
	return InstanceSummaryResponse{
		ID:                   inst.ID,
		WorkflowDefinitionID: inst.WorkflowDefinitionID,
		Status:               string(inst.Status),
		WaitingReason:        nullableString(string(inst.WaitingReason)),
		PauseRequested:       inst.PauseRequested,
		TerminationPending:   inst.TerminationPending,
		Error:                nullableString(inst.Error),
		StartedAt:            inst.StartedAt,
		FinishedAt:           inst.FinishedAt,
		CreatedBy:            inst.CreatedBy,
		UpdatedBy:            inst.UpdatedBy,
		CreatedAt:            inst.CreatedAt,
		UpdatedAt:            inst.UpdatedAt,
	}
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nodeInstanceIDString(instanceID string, occurrence *string) *string {
	if occurrence == nil {
		return nil
	}
	id := model.NodeInstanceID{WorkflowInstanceID: instanceID, OccurrenceID: *occurrence}.String()
	return &id
}

// toNodeDebugResponse maps the service detail onto the openapi NodeDebug
// schema. nil raw messages render as JSON null.
func toNodeDebugResponse(d *service.NodeDebugDetail) NodeDebugResponse {
	return NodeDebugResponse{
		OccurrenceID:           d.OccurrenceID,
		SourceNodeDefinitionID: d.SourceNodeDefinitionID,
		Name:                   d.Name,
		Type:                   d.Type,
		SelectedAttempt:        d.SelectedAttempt,
		LatestAttempt:          d.LatestAttempt,
		AttemptCount:           d.AttemptCount,
		Status:                 d.Status,
		ContextBefore:          d.ContextBefore,
		ContextAfter:           d.ContextAfter,
		Input:                  d.Input,
		Output:                 d.Output,
		Error:                  d.Error,
		RecoveryPolicy:         d.RecoveryPolicy,
		RecoveryResult:         d.RecoveryResult,
		Cancelled:              d.Cancelled,
		StartedAt:              d.StartedAt,
		FinishedAt:             d.FinishedAt,
		StoppedAt:              d.StoppedAt,
		DurationMS:             d.DurationMS,
		CreatedAt:              d.CreatedAt,
		UpdatedAt:              d.UpdatedAt,
	}
}
