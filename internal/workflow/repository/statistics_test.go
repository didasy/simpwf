package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/internal/workflow/repository"
)

func TestStatisticsSummaryCounts(t *testing.T) {
	db := setupTestDB(t)
	seedInstanceFixture(t, db)
	ctx := context.Background()
	repo := repository.NewInstanceRepository(db)

	now := time.Now().UTC().Truncate(time.Second)
	day1 := now.Add(-48 * time.Hour)
	day2 := now.Add(-24 * time.Hour)

	mk := func(id string, status model.WorkflowStatus, created time.Time, withTimes bool) model.WorkflowInstance {
		w := newTestInstance(id, status, "")
		w.CreatedAt = created
		w.UpdatedAt = created
		if withTimes {
			start := created
			fin := created.Add(90 * time.Second)
			w.StartedAt = &start
			w.FinishedAt = &fin
		}
		return w
	}

	// Day 1: 1 finished (with timestamps), 1 failed (with timestamps), 1 running.
	insertInstance(t, db, mk("11111111-1111-7111-8111-111111111111", model.WorkflowFinished, day1, true))
	failed := mk("22222222-2222-7222-8222-222222222222", model.WorkflowFailed, day1, true)
	failed.Error = "boom"
	insertInstance(t, db, failed)
	insertInstance(t, db, mk("33333333-3333-7333-8333-333333333333", model.WorkflowRunning, day1, false))
	// Day 2: 1 stopped (with timestamps), 1 waiting.
	insertInstance(t, db, mk("44444444-4444-7444-8444-444444444444", model.WorkflowStopped, day2, true))
	insertInstance(t, db, mk("55555555-5555-7555-8555-555555555555", model.WorkflowWaiting, day2, false))

	from := day1.Add(-time.Hour)
	to := now.Add(time.Hour)
	got, err := repo.StatisticsSummary(ctx, repository.StatisticsQuery{From: &from, To: &to})
	if err != nil {
		t.Fatalf("StatisticsSummary() error = %v", err)
	}
	if got.TotalRuns != 5 {
		t.Errorf("TotalRuns = %d, want 5", got.TotalRuns)
	}
	if got.FinishedRuns != 1 || got.FailedRuns != 1 || got.StoppedRuns != 1 {
		t.Errorf("finished/failed/stopped = %d/%d/%d, want 1/1/1", got.FinishedRuns, got.FailedRuns, got.StoppedRuns)
	}
	if got.TerminalRuns != 3 {
		t.Errorf("TerminalRuns = %d, want 3", got.TerminalRuns)
	}
	// Active = waiting + running + paused: 1 running (day1) + 1 waiting (day2).
	if got.ActiveRuns != 2 {
		t.Errorf("ActiveRuns = %d, want 2", got.ActiveRuns)
	}
	if got.SuccessRate == nil || *got.SuccessRate < 0.333 || *got.SuccessRate > 0.334 {
		t.Errorf("SuccessRate = %v, want ~1/3", got.SuccessRate)
	}
	// Three terminal runs each lasting 90s → avg 90000ms.
	if got.AverageDurationMS == nil || *got.AverageDurationMS != 90000 {
		t.Errorf("AverageDurationMS = %v, want 90000", got.AverageDurationMS)
	}
	if len(got.RunsPerDay) != 2 {
		t.Fatalf("RunsPerDay len = %d, want 2", len(got.RunsPerDay))
	}
	// Default order is descending by date: day2 bucket first.
	if got.RunsPerDay[0].Total != 2 || got.RunsPerDay[1].Total != 3 {
		t.Errorf("RunsPerDay totals = %+v, want day2=2 then day1=3", got.RunsPerDay)
	}
	if got.RunsPerDay[0].Date == "" || got.RunsPerDay[1].Date == "" {
		t.Errorf("RunsPerDay dates must be YYYY-MM-DD, got %+v", got.RunsPerDay)
	}

	// Narrow window to day 2 only (inclusive bounds).
	from2 := day2.Add(-time.Hour)
	to2 := day2.Add(time.Hour)
	day2only, err := repo.StatisticsSummary(ctx, repository.StatisticsQuery{From: &from2, To: &to2})
	if err != nil {
		t.Fatalf("StatisticsSummary(day2) error = %v", err)
	}
	if day2only.TotalRuns != 2 {
		t.Errorf("day2 TotalRuns = %d, want 2", day2only.TotalRuns)
	}
	if day2only.ActiveRuns != 1 {
		t.Errorf("day2 ActiveRuns = %d, want 1", day2only.ActiveRuns)
	}

	// Empty window: zero counts, nil rate/avg.
	from3 := now.Add(time.Hour)
	to3 := now.Add(2 * time.Hour)
	empty, err := repo.StatisticsSummary(ctx, repository.StatisticsQuery{From: &from3, To: &to3})
	if err != nil {
		t.Fatalf("StatisticsSummary(empty) error = %v", err)
	}
	if empty.TotalRuns != 0 || empty.TerminalRuns != 0 || empty.ActiveRuns != 0 {
		t.Errorf("empty window = %+v, want zeros", empty)
	}
	if empty.SuccessRate != nil || empty.AverageDurationMS != nil {
		t.Errorf("empty window rate/avg = %v/%v, want nil/nil", empty.SuccessRate, empty.AverageDurationMS)
	}
	if len(empty.RunsPerDay) != 0 {
		t.Errorf("empty window RunsPerDay = %+v, want empty", empty.RunsPerDay)
	}
}
