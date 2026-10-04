package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrParallelExecutionNotFound = errors.New("repository: parallel execution not found")
	ErrParallelBranchNotFound    = errors.New("repository: parallel branch not found")
)

// ForkBranch is one branch to create at fork: its stable name, deterministic
// index, entry node, initial cursor, and forked context snapshot.
type ForkBranch struct {
	Name        string
	BranchIndex int
	StartNodeID string
	Frame       model.Frame
	Context     json.RawMessage
}

// ForkParallel atomically parks the forking scope on its join and creates
// one parallel execution with its branches. A top-level fork (nil
// ParentBranchID) parks the instance and enqueues a status update; a
// nested fork parks the parent branch instead and leaves the instance row
// untouched. Frame is the parked cursor (at the join node, group stack
// preserved) and Context is the frozen snapshot. The park is fenced on
// Revision/WorkerID like a checkpoint.
type ForkParallel struct {
	InstanceID           string
	WorkflowDefinitionID string
	WorkerID             string
	Revision             int64
	ParentBranchID       *string
	Depth                int
	StartNodeID          string
	EndNodeID            string
	Frame                model.Frame
	Counters             model.Counters
	Context              json.RawMessage
	Branches             []ForkBranch
	// Debug parks the debug step-through: the top-level parent lands
	// paused and the branches are born paused so each resume steps
	// exactly one.
	Debug bool
}

// BranchCheckpoint is the fenced write that commits one branch transition.
type BranchCheckpoint struct {
	BranchID      string
	WorkerID      string
	Revision      int64
	Status        model.ParallelBranchStatus
	WaitingReason model.WaitingReason
	Frame         model.Frame
	Counters      model.Counters
	Context       json.RawMessage
	Error         string
}

// ParallelRepository is the durable state store for parallel executions and
// their branches: fork creation, branch claiming, fenced branch writes, and
// the atomic join barrier.
type ParallelRepository interface {
	// Fork parks the forking scope and creates an execution with branches
	// in one transaction: the instance for a top-level fork, the parent
	// branch for a nested one. The parked row must be running and leased
	// to WorkerID at Revision; otherwise ErrLeaseLost or
	// ErrRevisionConflict. On a debug instance the top-level parent parks
	// paused and the branches are born paused; a nested owner still parks
	// barrier-waiting so the barrier can wake it.
	Fork(ctx context.Context, f ForkParallel) (*model.ParallelExecution, []model.ParallelBranch, error)
	// GetExecution loads one execution or ErrParallelExecutionNotFound.
	GetExecution(ctx context.Context, id string) (*model.ParallelExecution, error)
	// ListExecutions returns an instance's executions in creation order.
	ListExecutions(ctx context.Context, instanceID string) ([]model.ParallelExecution, error)
	// GetBranch loads one branch or ErrParallelBranchNotFound.
	GetBranch(ctx context.Context, id string) (*model.ParallelBranch, error)
	// ListBranches returns an execution's branches ordered by branch_index.
	ListBranches(ctx context.Context, executionID string) ([]model.ParallelBranch, error)
	// ListBranchIDs returns every branch id of an instance, used to fan
	// termination cancels out to in-flight branch workers.
	ListBranchIDs(ctx context.Context, instanceID string) ([]string, error)
	// ClaimNextBranches leases up to limit runnable branches (pending,
	// waiting with no active lease, or running with an expired lease)
	// using FOR UPDATE SKIP LOCKED, moving them to running.
	ClaimNextBranches(ctx context.Context, workerID string, lease time.Duration, limit int) ([]model.ParallelBranch, error)
	// CheckpointBranch commits a branch transition. It succeeds only while
	// the worker still holds the lease at the expected revision; otherwise
	// it returns ErrLeaseLost or ErrRevisionConflict.
	CheckpointBranch(ctx context.Context, c BranchCheckpoint) error
	// WakePausedBranch flips one debug-parked branch (waiting/paused) back
	// to runnable so the next claim runs a single step. It returns
	// ErrStatusConflict when the branch is not paused and
	// ErrParallelBranchNotFound when missing.
	WakePausedBranch(ctx context.Context, branchID string) error
	// RenewBranchLease extends a branch lease; ErrLeaseLost when lost.
	RenewBranchLease(ctx context.Context, branchID, workerID string, lease time.Duration) error
	// RenewBranchLeases extends every branch lease held by a worker.
	RenewBranchLeases(ctx context.Context, workerID string, lease time.Duration) error
	// CompleteBranch records a branch arrival at the join, persisting the
	// final cursor, counters, and context the join reads. The branch write
	// is fenced; a retry of an already-completed branch is a no-op. When
	// the branch was the last incomplete one, the execution moves to
	// ready_to_join and the parent wakes to runnable in the same
	// transaction, and ready is true exactly once.
	CompleteBranch(ctx context.Context, c BranchCompletion) (ready bool, err error)
	// FailBranch records an unrecovered branch failure and propagates it in
	// one transaction: the branch fails (fenced), its execution fails,
	// live siblings and nested subtrees cancel, the failure climbs through
	// nested owners, and a parked top-level parent fails with the leaf
	// error. Retrying an already-terminal branch is a no-op. failedParent
	// reports whether the top-level parent transitioned to failed.
	FailBranch(ctx context.Context, branchID, workerID string, revision int64, errMsg string) (failedParent bool, err error)
	// FailExecution marks an execution failed unless already resolved.
	// Idempotent; ErrParallelExecutionNotFound when missing.
	FailExecution(ctx context.Context, executionID string) error
	// JoinParallel commits the join: the fenced parent advance plus the
	// execution completion in one transaction. The execution must be
	// ready_to_join, otherwise ErrStatusConflict: the join arms exactly
	// once.
	JoinParallel(ctx context.Context, j JoinCheckpoint) error
	// JoinBranchParallel commits a nested join: the fenced parent-branch
	// advance plus the nested execution completion in one transaction.
	// Same readiness rule as JoinParallel; the fence guards the branch
	// row, and the instance row is untouched.
	JoinBranchParallel(ctx context.Context, j JoinBranchCheckpoint) error
	// CountActiveBranches returns the number of non-terminal branches of
	// an instance across all its executions.
	CountActiveBranches(ctx context.Context, instanceID string) (int64, error)
}

type parallelRepo struct {
	db *gorm.DB
}

// NewParallelRepository builds the parallel execution repository.
func NewParallelRepository(db *gorm.DB) ParallelRepository {
	return &parallelRepo{db: db}
}

const forkParentSQL = `
UPDATE workflow_instances
SET status = ?, waiting_reason = ?, frame = ?, counters = ?, context = ?,
    leased_by = '', lease_expiry = NULL,
    revision = revision + 1, updated_at = now()
WHERE id = ? AND revision = ? AND leased_by = ?`

func (r *parallelRepo) Fork(ctx context.Context, f ForkParallel) (*model.ParallelExecution, []model.ParallelBranch, error) {
	frame, err := f.Frame.JSON()
	if err != nil {
		return nil, nil, err
	}
	counters, err := f.Counters.JSON()
	if err != nil {
		return nil, nil, err
	}
	debug := f.Debug
	now := time.Now().UTC()
	ex := model.ParallelExecution{
		ID:             newRepoID(),
		InstanceID:     f.InstanceID,
		ParentBranchID: f.ParentBranchID,
		Depth:          f.Depth,
		StartNodeID:    f.StartNodeID,
		EndNodeID:      f.EndNodeID,
		Status:         model.ParallelWaitingForBranches,
		BranchCount:    len(f.Branches),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	branchStatus, branchReason := model.ParallelBranchPending, model.WaitingReasonRunnable
	if debug {
		branchStatus, branchReason = model.ParallelBranchWaiting, model.WaitingReasonPaused
	}
	branches := make([]model.ParallelBranch, 0, len(f.Branches))
	for _, b := range f.Branches {
		fr, err := b.Frame.JSON()
		if err != nil {
			return nil, nil, err
		}
		branches = append(branches, model.ParallelBranch{
			ID:                  newRepoID(),
			ParallelExecutionID: ex.ID,
			InstanceID:          f.InstanceID,
			Name:                b.Name,
			BranchIndex:         b.BranchIndex,
			StartNodeID:         b.StartNodeID,
			Frame:               fr,
			Context:             b.Context,
			Status:              branchStatus,
			WaitingReason:       branchReason,
			CreatedAt:           now,
			UpdatedAt:           now,
		})
	}
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if f.ParentBranchID != nil {
			res := tx.Exec(branchCheckpointSQL,
				string(model.ParallelBranchWaiting), string(model.WaitingReasonParallel),
				frame, counters, jsonCol(f.Context, "{}"), "",
				*f.ParentBranchID, f.Revision, f.WorkerID,
			)
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected != 1 {
				return diagnoseParallelFence(ctx, tx, "parallel_branches", *f.ParentBranchID, f.WorkerID, f.Revision, ErrParallelBranchNotFound)
			}
		} else {
			parentStatus := model.WorkflowWaiting
			if debug {
				parentStatus = model.WorkflowPaused
			}
			res := tx.Exec(forkParentSQL,
				string(parentStatus), string(model.WaitingReasonParallel),
				frame, counters, jsonCol(f.Context, "{}"),
				f.InstanceID, f.Revision, f.WorkerID,
			)
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected != 1 {
				return diagnoseParallelFence(ctx, tx, "workflow_instances", f.InstanceID, f.WorkerID, f.Revision, ErrInstanceNotFound)
			}
		}
		if err := tx.Create(&[]ParallelExecutionModel{ParallelExecutionToModel(ex)}).Error; err != nil {
			return err
		}
		models := make([]ParallelBranchModel, 0, len(branches))
		for _, b := range branches {
			m := ParallelBranchToModel(b)
			models = append(models, m)
		}
		if err := tx.Create(&models).Error; err != nil {
			return err
		}
		if f.ParentBranchID != nil {
			// A nested fork is not an instance transition: no row
			// change, no status update.
			return nil
		}
		from := statusWithReason{status: model.WorkflowRunning, waitingReason: model.WaitingReasonRunnable}
		to := statusWithReason{status: model.WorkflowWaiting, waitingReason: model.WaitingReasonParallel}
		if debug {
			to.status = model.WorkflowPaused
		}
		return enqueueStatusUpdateTx(ctx, tx, f.InstanceID, f.WorkflowDefinitionID, f.Revision+1, from, to, transitionEvents(from, to), "", f.Context, now)
	})
	if err != nil {
		return nil, nil, err
	}
	return &ex, branches, nil
}

func (r *parallelRepo) GetExecution(ctx context.Context, id string) (*model.ParallelExecution, error) {
	var m ParallelExecutionModel
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrParallelExecutionNotFound
		}
		return nil, err
	}
	ex := ParallelExecutionFromModel(m)
	return &ex, nil
}

func (r *parallelRepo) ListExecutions(ctx context.Context, instanceID string) ([]model.ParallelExecution, error) {
	var models []ParallelExecutionModel
	if err := r.db.WithContext(ctx).Where("instance_id = ?", instanceID).
		Order("created_at").Order("id").Find(&models).Error; err != nil {
		return nil, err
	}
	out := make([]model.ParallelExecution, len(models))
	for i := range models {
		out[i] = ParallelExecutionFromModel(models[i])
	}
	return out, nil
}

func (r *parallelRepo) ListBranchIDs(ctx context.Context, instanceID string) ([]string, error) {
	var ids []string
	if err := r.db.WithContext(ctx).Model(&ParallelBranchModel{}).
		Where("instance_id = ?", instanceID).Pluck("id", &ids).Error; err != nil {
		return nil, err
	}
	return ids, nil
}

func (r *parallelRepo) GetBranch(ctx context.Context, id string) (*model.ParallelBranch, error) {
	var m ParallelBranchModel
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&m).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrParallelBranchNotFound
		}
		return nil, err
	}
	b := ParallelBranchFromModel(m)
	return &b, nil
}

func (r *parallelRepo) ListBranches(ctx context.Context, executionID string) ([]model.ParallelBranch, error) {
	var models []ParallelBranchModel
	if err := r.db.WithContext(ctx).Where("parallel_execution_id = ?", executionID).
		Order("branch_index").Find(&models).Error; err != nil {
		return nil, err
	}
	out := make([]model.ParallelBranch, len(models))
	for i := range models {
		out[i] = ParallelBranchFromModel(models[i])
	}
	return out, nil
}

func (r *parallelRepo) ClaimNextBranches(ctx context.Context, workerID string, lease time.Duration, limit int) ([]model.ParallelBranch, error) {
	tx := r.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	defer tx.Rollback()

	var models []ParallelBranchModel
	err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
		Where(`status = ?
		       OR (status = ? AND waiting_reason = ? AND (lease_expiry IS NULL OR lease_expiry < now()))
		       OR (status = ? AND lease_expiry < now())`,
			string(model.ParallelBranchPending),
			string(model.ParallelBranchWaiting), string(model.WaitingReasonRunnable),
			string(model.ParallelBranchRunning)).
		Order("updated_at").
		Limit(limit).
		Find(&models).Error
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	expiry := now.Add(lease)
	for i := range models {
		m := &models[i]
		m.Status = string(model.ParallelBranchRunning)
		m.LeasedBy = workerID
		m.LeaseExpiry = &expiry
		m.Revision++
		m.UpdatedAt = now
		if err := tx.Save(m).Error; err != nil {
			return nil, err
		}
	}
	if err := tx.Commit().Error; err != nil {
		return nil, err
	}

	out := make([]model.ParallelBranch, len(models))
	for i := range models {
		out[i] = ParallelBranchFromModel(models[i])
	}
	return out, nil
}

const branchCheckpointSQL = `
UPDATE parallel_branches
SET status = ?, waiting_reason = ?, frame = ?, counters = ?, context = ?, error = ?,
    leased_by = '', lease_expiry = NULL,
    revision = revision + 1, updated_at = now()
WHERE id = ? AND revision = ? AND leased_by = ?`

func (r *parallelRepo) CheckpointBranch(ctx context.Context, c BranchCheckpoint) error {
	frame, err := c.Frame.JSON()
	if err != nil {
		return err
	}
	counters, err := c.Counters.JSON()
	if err != nil {
		return err
	}
	res := r.db.WithContext(ctx).Exec(branchCheckpointSQL,
		string(c.Status), string(c.WaitingReason), frame, counters, jsonCol(c.Context, "{}"), c.Error,
		c.BranchID, c.Revision, c.WorkerID,
	)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		return diagnoseParallelFence(ctx, r.db.WithContext(ctx), "parallel_branches", c.BranchID, c.WorkerID, c.Revision, ErrParallelBranchNotFound)
	}
	return nil
}

func (r *parallelRepo) RenewBranchLease(ctx context.Context, branchID, workerID string, lease time.Duration) error {
	res := r.db.WithContext(ctx).Model(&ParallelBranchModel{}).
		Where("id = ? AND leased_by = ? AND status = ?", branchID, workerID, string(model.ParallelBranchRunning)).
		UpdateColumn("lease_expiry", gorm.Expr("now() + ? * interval '1 second'", int(lease.Seconds())))
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return ErrLeaseLost
	}
	return nil
}

func (r *parallelRepo) RenewBranchLeases(ctx context.Context, workerID string, lease time.Duration) error {
	res := r.db.WithContext(ctx).Model(&ParallelBranchModel{}).
		Where("leased_by = ? AND status = ?", workerID, string(model.ParallelBranchRunning)).
		UpdateColumn("lease_expiry", gorm.Expr("now() + ? * interval '1 second'", int(lease.Seconds())))
	return res.Error
}

// BranchCompletion carries a branch arrival at the join: the fence plus the
// final cursor, counters, and context the join reads.
type BranchCompletion struct {
	BranchID string
	WorkerID string
	Revision int64
	Frame    model.Frame
	Counters model.Counters
	Context  json.RawMessage
}

func (r *parallelRepo) CompleteBranch(ctx context.Context, c BranchCompletion) (bool, error) {
	frame, err := c.Frame.JSON()
	if err != nil {
		return false, err
	}
	counters, err := c.Counters.JSON()
	if err != nil {
		return false, err
	}
	ready := false
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var b ParallelBranchModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", c.BranchID).First(&b).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrParallelBranchNotFound
			}
			return err
		}
		// A retry of a committed completion is a no-op: the barrier
		// already counted this branch exactly once.
		if b.Status == string(model.ParallelBranchCompleted) {
			return nil
		}
		if b.LeasedBy != c.WorkerID {
			return ErrLeaseLost
		}
		if b.Revision != c.Revision {
			return ErrRevisionConflict
		}
		if b.Status != string(model.ParallelBranchRunning) && b.Status != string(model.ParallelBranchWaiting) {
			return ErrStatusConflict
		}
		now := time.Now().UTC()
		if err := tx.Model(&ParallelBranchModel{}).Where("id = ?", c.BranchID).Updates(map[string]any{
			"status":       string(model.ParallelBranchCompleted),
			"frame":        frame,
			"counters":     counters,
			"context":      jsonCol(c.Context, "{}"),
			"leased_by":    "",
			"lease_expiry": nil,
			"revision":     gorm.Expr("revision + 1"),
			"updated_at":   now,
		}).Error; err != nil {
			return err
		}
		var ex ParallelExecutionModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", b.ParallelExecutionID).First(&ex).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrParallelExecutionNotFound
			}
			return err
		}
		if ex.Status != string(model.ParallelWaitingForBranches) {
			// The execution already resolved (failed/cancelled/ready);
			// this arrival changes nothing.
			return nil
		}
		completed := ex.CompletedCount + 1
		updates := map[string]any{"completed_count": completed, "updated_at": now}
		if completed >= ex.BranchCount {
			updates["status"] = string(model.ParallelReadyToJoin)
			ready = true
		}
		if err := tx.Model(&ParallelExecutionModel{}).Where("id = ?", ex.ID).Updates(updates).Error; err != nil {
			return err
		}
		if !ready {
			return nil
		}
		// Wake whoever owns this execution: the parked parent branch for
		// a nested execution, else the parked parent instance. The guards
		// keep a concurrent stop/cancel authoritative.
		if ex.ParentBranchID != nil {
			return tx.Exec(`UPDATE parallel_branches
				SET waiting_reason = ?, updated_at = now()
				WHERE id = ? AND status = ? AND waiting_reason = ?`,
				string(model.WaitingReasonRunnable),
				*ex.ParentBranchID, string(model.ParallelBranchWaiting), string(model.WaitingReasonParallel),
			).Error
		}
		// A paused parent (debug step-through or a manual pause) keeps
		// its status but still flips runnable: the later resume lands
		// claimable instead of stuck on parallel.
		return tx.Exec(`UPDATE workflow_instances
			SET waiting_reason = ?, updated_at = now()
			WHERE id = ? AND status IN ? AND waiting_reason = ?`,
			string(model.WaitingReasonRunnable),
			ex.InstanceID, []string{string(model.WorkflowWaiting), string(model.WorkflowPaused)}, string(model.WaitingReasonParallel),
		).Error
	})
	if err != nil {
		return false, err
	}
	return ready, nil
}

func parallelBranchTerminal(status string) bool {
	return status == string(model.ParallelBranchCompleted) ||
		status == string(model.ParallelBranchFailed) ||
		status == string(model.ParallelBranchCancelled)
}

func parallelExecutionTerminal(status string) bool {
	return status == string(model.ParallelExecutionCompleted) ||
		status == string(model.ParallelExecutionFailed) ||
		status == string(model.ParallelExecutionCancelled)
}

func (r *parallelRepo) FailBranch(ctx context.Context, branchID, workerID string, revision int64, errMsg string) (bool, error) {
	failedParent := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var leaf ParallelBranchModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", branchID).First(&leaf).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrParallelBranchNotFound
			}
			return err
		}
		if parallelBranchTerminal(leaf.Status) {
			return nil
		}
		if leaf.LeasedBy != workerID {
			return ErrLeaseLost
		}
		if leaf.Revision != revision {
			return ErrRevisionConflict
		}
		if leaf.Status != string(model.ParallelBranchRunning) && leaf.Status != string(model.ParallelBranchWaiting) {
			return ErrStatusConflict
		}
		fp, err := failBranchCascadeTx(ctx, tx, leaf, errMsg, time.Now().UTC())
		if err != nil {
			return err
		}
		failedParent = fp
		return nil
	})
	if err != nil {
		return false, err
	}
	return failedParent, nil
}

// failBranchCascadeTx fails a locked leaf branch and propagates the failure:
// its execution fails, live siblings and nested subtrees cancel, the failure
// climbs through nested owners, and a parked top-level parent fails with the
// leaf error. Callers lock and validate the leaf first; lease checks stay
// with them because parked branches fail without holding a lease.
func failBranchCascadeTx(ctx context.Context, tx *gorm.DB, leaf ParallelBranchModel, errMsg string, now time.Time) (bool, error) {
	failedParent := false
	if err := tx.Model(&ParallelBranchModel{}).Where("id = ?", leaf.ID).Updates(map[string]any{
		"status":       string(model.ParallelBranchFailed),
		"error":        errMsg,
		"leased_by":    "",
		"lease_expiry": nil,
		"revision":     gorm.Expr("revision + 1"),
		"updated_at":   now,
	}).Error; err != nil {
		return false, err
	}
	var ex ParallelExecutionModel
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", leaf.ParallelExecutionID).First(&ex).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, ErrParallelExecutionNotFound
		}
		return false, err
	}
	if parallelExecutionTerminal(ex.Status) {
		// Already resolved; its cascade ran.
		return false, nil
	}
	if err := tx.Model(&ParallelExecutionModel{}).Where("id = ?", ex.ID).Updates(map[string]any{
		"status":     string(model.ParallelExecutionFailed),
		"updated_at": now,
	}).Error; err != nil {
		return false, err
	}
	if err := cancelExecutionSubtreeTx(tx, ex.ID, now); err != nil {
		return false, err
	}
	// Climb through nested owners: each parked owner fails, its
	// execution fails, and its live siblings cancel.
	for ex.ParentBranchID != nil {
		var owner ParallelBranchModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", *ex.ParentBranchID).First(&owner).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				break
			}
			return false, err
		}
		if parallelBranchTerminal(owner.Status) {
			break
		}
		if err := tx.Model(&ParallelBranchModel{}).Where("id = ?", owner.ID).Updates(map[string]any{
			"status":       string(model.ParallelBranchFailed),
			"error":        errMsg,
			"leased_by":    "",
			"lease_expiry": nil,
			"revision":     gorm.Expr("revision + 1"),
			"updated_at":   now,
		}).Error; err != nil {
			return false, err
		}
		var parentEx ParallelExecutionModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", owner.ParallelExecutionID).First(&parentEx).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				break
			}
			return false, err
		}
		if parallelExecutionTerminal(parentEx.Status) {
			break
		}
		if err := tx.Model(&ParallelExecutionModel{}).Where("id = ?", parentEx.ID).Updates(map[string]any{
			"status":     string(model.ParallelExecutionFailed),
			"updated_at": now,
		}).Error; err != nil {
			return false, err
		}
		if err := cancelExecutionSubtreeTx(tx, parentEx.ID, now); err != nil {
			return false, err
		}
		ex = parentEx
	}
	// Fail the parked top-level parent, waiting or paused (debug
	// step-through or a manual pause). Any other state (stopped,
	// spuriously woken) keeps its owner authoritative.
	parentErr := "parallel branch \"" + leaf.Name + "\" failed: " + errMsg
	var preStatus string
	if err := tx.Table("workflow_instances").Select("status").Where("id = ?", ex.InstanceID).Scan(&preStatus).Error; err != nil {
		return false, err
	}
	res := tx.Exec(`UPDATE workflow_instances
			SET status = ?, error = ?, finished_at = now(), revision = revision + 1, updated_at = now()
			WHERE id = ? AND status IN ? AND waiting_reason = ?`,
		string(model.WorkflowFailed), parentErr,
		ex.InstanceID, []string{string(model.WorkflowWaiting), string(model.WorkflowPaused)}, string(model.WaitingReasonParallel),
	)
	if res.Error != nil {
		return false, res.Error
	}
	if res.RowsAffected != 1 {
		return false, nil
	}
	failedParent = true
	var parent struct {
		WorkflowDefinitionID string
		Revision             int64
		Context              []byte
	}
	if err := tx.Table("workflow_instances").Select("workflow_definition_id, revision, context").
		Where("id = ?", ex.InstanceID).Scan(&parent).Error; err != nil {
		return false, err
	}
	from := statusWithReason{status: model.WorkflowStatus(preStatus), waitingReason: model.WaitingReasonParallel}
	to := statusWithReason{status: model.WorkflowFailed}
	if err := enqueueStatusUpdateTx(ctx, tx, ex.InstanceID, parent.WorkflowDefinitionID, parent.Revision, from, to, transitionEvents(from, to), parentErr, parent.Context, now); err != nil {
		return false, err
	}
	return failedParent, nil
}

// cancelExecutionSubtreeTx cancels every live branch of an execution and,
// recursively, every live execution nested under its branches. Lease clears
// fence in-flight workers.
func cancelExecutionSubtreeTx(tx *gorm.DB, executionID string, now time.Time) error {
	execIDs := []string{executionID}
	liveBranch := []string{
		string(model.ParallelBranchPending),
		string(model.ParallelBranchRunning),
		string(model.ParallelBranchWaiting),
	}
	liveExecution := []string{
		string(model.ParallelWaitingForBranches),
		string(model.ParallelReadyToJoin),
	}
	for len(execIDs) > 0 {
		var allBranches []string
		if err := tx.Model(&ParallelBranchModel{}).Where("parallel_execution_id IN ?", execIDs).Pluck("id", &allBranches).Error; err != nil {
			return err
		}
		if err := tx.Model(&ParallelBranchModel{}).
			Where("parallel_execution_id IN ? AND status IN ?", execIDs, liveBranch).
			Updates(map[string]any{
				"status":       string(model.ParallelBranchCancelled),
				"leased_by":    "",
				"lease_expiry": nil,
				"revision":     gorm.Expr("revision + 1"),
				"updated_at":   now,
			}).Error; err != nil {
			return err
		}
		execIDs = nil
		if len(allBranches) == 0 {
			continue
		}
		if err := tx.Model(&ParallelExecutionModel{}).
			Where("parent_branch_id IN ? AND status IN ?", allBranches, liveExecution).
			Pluck("id", &execIDs).Error; err != nil {
			return err
		}
		if len(execIDs) == 0 {
			continue
		}
		if err := tx.Model(&ParallelExecutionModel{}).Where("id IN ?", execIDs).Updates(map[string]any{
			"status":     string(model.ParallelExecutionCancelled),
			"updated_at": now,
		}).Error; err != nil {
			return err
		}
	}
	return nil
}

// JoinCheckpoint is the atomic join commit: a fenced parent transition plus
// the execution it completes.
type JoinCheckpoint struct {
	Checkpoint
	ExecutionID string
}

func (r *parallelRepo) JoinParallel(ctx context.Context, j JoinCheckpoint) error {
	frame, err := j.Frame.JSON()
	if err != nil {
		return err
	}
	counters, err := j.Counters.JSON()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ex ParallelExecutionModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", j.ExecutionID).First(&ex).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrParallelExecutionNotFound
			}
			return err
		}
		if ex.Status != string(model.ParallelReadyToJoin) {
			return ErrStatusConflict
		}
		if err := execInstanceCheckpointTx(ctx, tx, j.Checkpoint, frame, counters, now); err != nil {
			return err
		}
		return tx.Model(&ParallelExecutionModel{}).Where("id = ?", ex.ID).Updates(map[string]any{
			"status":     string(model.ParallelExecutionCompleted),
			"updated_at": now,
		}).Error
	})
}

// JoinBranchCheckpoint is the atomic nested join commit: a fenced
// parent-branch transition plus the nested execution it completes.
type JoinBranchCheckpoint struct {
	BranchCheckpoint
	ExecutionID string
}

func (r *parallelRepo) JoinBranchParallel(ctx context.Context, j JoinBranchCheckpoint) error {
	frame, err := j.Frame.JSON()
	if err != nil {
		return err
	}
	counters, err := j.Counters.JSON()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ex ParallelExecutionModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", j.ExecutionID).First(&ex).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrParallelExecutionNotFound
			}
			return err
		}
		if ex.Status != string(model.ParallelReadyToJoin) {
			return ErrStatusConflict
		}
		c := j.BranchCheckpoint
		res := tx.Exec(branchCheckpointSQL,
			string(c.Status), string(c.WaitingReason), frame, counters, jsonCol(c.Context, "{}"), c.Error,
			c.BranchID, c.Revision, c.WorkerID,
		)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected != 1 {
			return diagnoseParallelFence(ctx, tx, "parallel_branches", c.BranchID, c.WorkerID, c.Revision, ErrParallelBranchNotFound)
		}
		return tx.Model(&ParallelExecutionModel{}).Where("id = ?", ex.ID).Updates(map[string]any{
			"status":     string(model.ParallelExecutionCompleted),
			"updated_at": now,
		}).Error
	})
}

func (r *parallelRepo) WakePausedBranch(ctx context.Context, branchID string) error {
	res := r.db.WithContext(ctx).Exec(`UPDATE parallel_branches
		SET waiting_reason = ?, updated_at = now()
		WHERE id = ? AND status = ? AND waiting_reason = ?`,
		string(model.WaitingReasonRunnable),
		branchID, string(model.ParallelBranchWaiting), string(model.WaitingReasonPaused),
	)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected != 1 {
		var n int64
		if cerr := r.db.WithContext(ctx).Table("parallel_branches").Where("id = ?", branchID).Count(&n).Error; cerr != nil {
			return cerr
		}
		if n == 0 {
			return ErrParallelBranchNotFound
		}
		return ErrStatusConflict
	}
	return nil
}

func (r *parallelRepo) FailExecution(ctx context.Context, executionID string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ex ParallelExecutionModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", executionID).First(&ex).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrParallelExecutionNotFound
			}
			return err
		}
		if ex.Status != string(model.ParallelWaitingForBranches) && ex.Status != string(model.ParallelReadyToJoin) {
			return nil
		}
		return tx.Model(&ParallelExecutionModel{}).Where("id = ?", ex.ID).Updates(map[string]any{
			"status":     string(model.ParallelExecutionFailed),
			"updated_at": time.Now().UTC(),
		}).Error
	})
}

func (r *parallelRepo) CountActiveBranches(ctx context.Context, instanceID string) (int64, error) {
	var n int64
	err := r.db.WithContext(ctx).Model(&ParallelBranchModel{}).
		Where("instance_id = ? AND status IN ?", instanceID, []string{
			string(model.ParallelBranchPending),
			string(model.ParallelBranchRunning),
			string(model.ParallelBranchWaiting),
		}).Count(&n).Error
	return n, err
}

// diagnoseParallelFence maps a zero-row fenced write to the precise cause.
func diagnoseParallelFence(ctx context.Context, g *gorm.DB, table, id, workerID string, revision int64, notFound error) error {
	var n int64
	if err := g.WithContext(ctx).Table(table).Where("id = ?", id).Count(&n).Error; err != nil {
		return err
	}
	if n == 0 {
		return notFound
	}
	var row struct {
		LeasedBy string
		Revision int64
	}
	if err := g.WithContext(ctx).Table(table).Select("leased_by, revision").Where("id = ?", id).Scan(&row).Error; err != nil {
		return err
	}
	if row.LeasedBy != workerID {
		return ErrLeaseLost
	}
	if row.Revision != revision {
		return ErrRevisionConflict
	}
	return ErrLeaseLost
}
