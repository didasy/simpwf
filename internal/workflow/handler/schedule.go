package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
	"github.com/simpwf/workflow-engine/pkg/ids"
)

// ScheduleHandler serves /v1/workflow/schedules routes.
type ScheduleHandler struct {
	svc service.ScheduleService
}

// NewScheduleHandler builds the handler.
func NewScheduleHandler(svc service.ScheduleService) *ScheduleHandler {
	return &ScheduleHandler{svc: svc}
}

// Create handles POST /v1/workflow/schedules.
//
// @Summary Create cron schedule
// @Tags workflow-schedules
// @Accept json
// @Produce json
// @Param request body CreateScheduleRequest true "Cron schedule"
// @Success 201 {object} ScheduleResponse
// @Failure 401,403,400,404,422,500 {object} Problem
// @Security ApiKeyAuth
// @Security BearerAuth
// @Router /v1/workflow/schedules [post]
func (h *ScheduleHandler) Create(c *gin.Context) {
	var req CreateScheduleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		WriteProblem(c, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if req.WorkflowDefinitionID == "" || req.Crontab == "" {
		WriteProblem(c, http.StatusUnprocessableEntity, "workflow_definition_id and crontab are required")
		return
	}
	sched, err := h.svc.Create(c.Request.Context(), service.CreateSchedule{
		WorkflowDefinitionID: req.WorkflowDefinitionID,
		Crontab:              req.Crontab,
		Timezone:             req.Timezone,
		Context:              req.Context,
		Enabled:              req.Enabled,
		Actor:                principalActor(c),
	})
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusCreated, toScheduleResponse(sched))
}

// List handles GET /v1/workflow/schedules.
//
// @Summary List cron schedules
// @Tags workflow-schedules
// @Produce json
// @Param page query int false "Page number"
// @Param per_page query int false "Items per page"
// @Success 200 {object} ListResponse[ScheduleResponse]
// @Failure 401,403,400,500 {object} Problem
// @Security ApiKeyAuth
// @Security BearerAuth
// @Router /v1/workflow/schedules [get]
func (h *ScheduleHandler) List(c *gin.Context) {
	page, perPage, err := parsePagination(c)
	if err != nil {
		WriteProblem(c, http.StatusBadRequest, err.Error())
		return
	}
	items, total, err := h.svc.List(c.Request.Context(), repository.ScheduleListQuery{Page: page, PerPage: perPage})
	if err != nil {
		WriteError(c, err)
		return
	}
	resp := make([]ScheduleResponse, 0, len(items))
	for _, sched := range items {
		resp = append(resp, toScheduleResponse(sched))
	}
	c.JSON(http.StatusOK, ListResponse[ScheduleResponse]{
		Items:      resp,
		Page:       page,
		PerPage:    perPage,
		Total:      total,
		TotalPages: totalPages(total, perPage),
	})
}

// Get handles GET /v1/workflow/schedules/{id}.
//
// @Summary Get cron schedule
// @Tags workflow-schedules
// @Produce json
// @Param id path string true "Schedule ID"
// @Success 200 {object} ScheduleResponse
// @Failure 401,403,400,404,500 {object} Problem
// @Security ApiKeyAuth
// @Security BearerAuth
// @Router /v1/workflow/schedules/{id} [get]
func (h *ScheduleHandler) Get(c *gin.Context) {
	id := c.Param("id")
	if !ids.Valid(id) {
		WriteProblem(c, http.StatusBadRequest, "id must be a valid uuid")
		return
	}
	sched, err := h.svc.Get(c.Request.Context(), id)
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusOK, toScheduleResponse(sched))
}

// Delete handles DELETE /v1/workflow/schedules/{id}.
//
// @Summary Delete cron schedule
// @Tags workflow-schedules
// @Produce json
// @Param id path string true "Schedule ID"
// @Success 204
// @Failure 401,403,400,404,500 {object} Problem
// @Security ApiKeyAuth
// @Security BearerAuth
// @Router /v1/workflow/schedules/{id} [delete]
func (h *ScheduleHandler) Delete(c *gin.Context) {
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

// Pause handles POST /v1/workflow/schedules/{id}/pause.
//
// @Summary Pause cron schedule
// @Tags workflow-schedules
// @Produce json
// @Param id path string true "Schedule ID"
// @Success 200 {object} ScheduleResponse
// @Failure 401,403,400,404,500 {object} Problem
// @Security ApiKeyAuth
// @Security BearerAuth
// @Router /v1/workflow/schedules/{id}/pause [post]
func (h *ScheduleHandler) Pause(c *gin.Context) {
	id := c.Param("id")
	if !ids.Valid(id) {
		WriteProblem(c, http.StatusBadRequest, "id must be a valid uuid")
		return
	}
	sched, err := h.svc.Pause(c.Request.Context(), service.ScheduleControl{ID: id, Actor: principalActor(c)})
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusOK, toScheduleResponse(sched))
}

// Resume handles POST /v1/workflow/schedules/{id}/resume.
//
// @Summary Resume cron schedule
// @Tags workflow-schedules
// @Produce json
// @Param id path string true "Schedule ID"
// @Success 200 {object} ScheduleResponse
// @Failure 401,403,400,404,500 {object} Problem
// @Security ApiKeyAuth
// @Security BearerAuth
// @Router /v1/workflow/schedules/{id}/resume [post]
func (h *ScheduleHandler) Resume(c *gin.Context) {
	id := c.Param("id")
	if !ids.Valid(id) {
		WriteProblem(c, http.StatusBadRequest, "id must be a valid uuid")
		return
	}
	sched, err := h.svc.Resume(c.Request.Context(), service.ScheduleControl{ID: id, Actor: principalActor(c)})
	if err != nil {
		WriteError(c, err)
		return
	}
	c.JSON(http.StatusOK, toScheduleResponse(sched))
}

func toScheduleResponse(sched model.CronSchedule) ScheduleResponse {
	return ScheduleResponse{
		ID:                   sched.ID,
		WorkflowDefinitionID: sched.WorkflowDefinitionID,
		Crontab:              sched.Crontab,
		Timezone:             sched.Timezone,
		Context:              sched.Context,
		Enabled:              sched.Enabled,
		CreatedBy:            sched.CreatedBy,
		UpdatedBy:            sched.UpdatedBy,
		CreatedAt:            sched.CreatedAt,
		UpdatedAt:            sched.UpdatedAt,
	}
}
