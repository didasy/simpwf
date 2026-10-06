package service

import (
	"context"

	"github.com/simpwf/workflow-engine/internal/workflow/auth"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

// StatisticsService is the use-case boundary for workflow statistics.
type StatisticsService interface {
	// Summary aggregates instance counts (terminal and active) over the
	// creation-time window, scoped to the caller's own runs. The service
	// principal, an admin, and an unauthenticated caller (authentication
	// disabled) see the unscoped aggregate.
	Summary(ctx context.Context, q repository.StatisticsQuery, p auth.Principal) (repository.StatisticsResult, error)
}

type statisticsService struct {
	instances repository.InstanceRepository
}

// NewStatisticsService builds the statistics service.
func NewStatisticsService(instances repository.InstanceRepository) StatisticsService {
	return &statisticsService{instances: instances}
}

func (s *statisticsService) Summary(ctx context.Context, q repository.StatisticsQuery, p auth.Principal) (repository.StatisticsResult, error) {
	return s.instances.StatisticsSummary(ctx, ownedStatistics(q, p))
}
