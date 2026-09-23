package handler

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
	"github.com/simpwf/workflow-engine/internal/workflow/service"
)

// statisticsServiceContract is the subset of service.StatisticsService the
// handler needs. It mirrors the interface so tests can fake it.
type statisticsServiceContract interface {
	Summary(ctx context.Context, q repository.StatisticsQuery) (repository.StatisticsResult, error)
}

// StatisticsHandler serves the statistics routes.
type StatisticsHandler struct {
	svc statisticsServiceContract
}

// NewStatisticsHandler builds the handler. It accepts the service
// interface so production wiring passes service.StatisticsService.
func NewStatisticsHandler(svc service.StatisticsService) *StatisticsHandler {
	return &StatisticsHandler{svc: svc}
}

// Summary handles GET /v1/statistics.
//
// @Summary Get workflow run statistics
// @Tags statistics
// @Produce json
// @Param created_from query string false "Inclusive window start (RFC3339)"
// @Param created_to query string false "Inclusive window end (RFC3339)"
// @Param order query string false "Runs-per-day direction: date or -date"
// @Success 200 {object} StatisticsSummaryResponse
// @Failure 400,500 {object} Problem
// @Security ApiKeyAuth
// @Router /v1/statistics [get]
func (h *StatisticsHandler) Summary(c *gin.Context) {
	q, err := ParseStatisticsSummaryQuery(c)
	if err != nil {
		WriteProblem(c, http.StatusBadRequest, err.Error())
		return
	}
	res, err := h.svc.Summary(c.Request.Context(), q.Window)
	if err != nil {
		WriteError(c, err)
		return
	}
	days := make([]RunsPerDayResponse, 0, len(res.RunsPerDay))
	for _, d := range res.RunsPerDay {
		days = append(days, RunsPerDayResponse{
			Date: d.Date, TotalRuns: d.Total,
			FinishedRuns: d.Finished, FailedRuns: d.Failed, StoppedRuns: d.Stopped,
		})
	}
	c.JSON(http.StatusOK, StatisticsSummaryResponse{
		TotalRuns: res.TotalRuns, FinishedRuns: res.FinishedRuns,
		FailedRuns: res.FailedRuns, StoppedRuns: res.StoppedRuns,
		TerminalRuns: res.TerminalRuns, ActiveRuns: res.ActiveRuns,
		SuccessRate:       res.SuccessRate,
		AverageDurationMS: res.AverageDurationMS, RunsPerDay: days,
	})
}
