package service

import (
	"context"

	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// StatisticsService is the use-case boundary for workflow statistics.
type StatisticsService interface {
	// Summary aggregates instance counts (terminal and active) over the
	// creation-time window.
	Summary(ctx context.Context, q repository.StatisticsQuery) (repository.StatisticsResult, error)
}

type statisticsService struct {
	instances repository.InstanceRepository
}

// NewStatisticsService builds the statistics service.
func NewStatisticsService(instances repository.InstanceRepository) StatisticsService {
	return &statisticsService{instances: instances}
}

func (s *statisticsService) Summary(ctx context.Context, q repository.StatisticsQuery) (repository.StatisticsResult, error) {
	return s.instances.StatisticsSummary(ctx, q)
}
