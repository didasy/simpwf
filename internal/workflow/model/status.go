package model

// WorkflowStatus is the lifecycle status of a workflow instance.
type WorkflowStatus string

const (
	WorkflowWaiting  WorkflowStatus = "waiting"
	WorkflowRunning  WorkflowStatus = "running"
	WorkflowPaused   WorkflowStatus = "paused"
	WorkflowFinished WorkflowStatus = "finished"
	WorkflowFailed   WorkflowStatus = "failed"
	WorkflowStopped  WorkflowStatus = "stopped"
)

// ValidWorkflowStatus reports whether s is a known workflow status.
func ValidWorkflowStatus(s WorkflowStatus) bool {
	switch s {
	case WorkflowWaiting, WorkflowRunning, WorkflowPaused, WorkflowFinished, WorkflowFailed, WorkflowStopped:
		return true
	default:
		return false
	}
}

// CanWorkflowTransition reports whether the state machine allows from -> to.
//
//	waiting --> running (claimed)
//	running --> waiting (checkpoint)
//	waiting --> paused (pause)
//	running --> paused (after node)
//	paused --> waiting (resume)
//	waiting --> stopped (stop)
//	running --> stopped (force stop)
//	paused --> stopped (stop)
//	running --> finished
//	running --> failed
func CanWorkflowTransition(from, to WorkflowStatus) bool {
	switch from {
	case WorkflowWaiting:
		return to == WorkflowRunning || to == WorkflowPaused || to == WorkflowStopped
	case WorkflowRunning:
		return to == WorkflowWaiting || to == WorkflowPaused || to == WorkflowStopped || to == WorkflowFinished || to == WorkflowFailed
	case WorkflowPaused:
		return to == WorkflowWaiting || to == WorkflowStopped
	default:
		return false // terminal statuses (finished, failed, stopped) are immutable
	}
}

// NodeStatus is the lifecycle status of a single node occurrence attempt set.
type NodeStatus string

const (
	NodeWaiting  NodeStatus = "waiting"
	NodeRunning  NodeStatus = "running"
	NodeFinished NodeStatus = "finished"
	NodeFailed   NodeStatus = "failed"
	NodeStopped  NodeStatus = "stopped"
)

// ValidNodeStatus reports whether s is a known node status.
func ValidNodeStatus(s NodeStatus) bool {
	switch s {
	case NodeWaiting, NodeRunning, NodeFinished, NodeFailed, NodeStopped:
		return true
	default:
		return false
	}
}

// CanNodeTransition reports whether the node state machine allows from -> to.
func CanNodeTransition(from, to NodeStatus) bool {
	switch from {
	case NodeWaiting:
		return to == NodeRunning || to == NodeStopped
	case NodeRunning:
		return to == NodeFinished || to == NodeFailed || to == NodeStopped
	default:
		return false
	}
}

// WaitingReason distinguishes why a waiting workflow instance is not
// schedulable. An empty value means runnable.
type WaitingReason string

const (
	// WaitingReasonRunnable marks a waiting instance that can be claimed.
	WaitingReasonRunnable WaitingReason = ""
	// WaitingReasonInput marks a waiting instance blocked on input delivery.
	WaitingReasonInput WaitingReason = "input"
)

// ValidWaitingReason reports whether r is a known waiting reason.
func ValidWaitingReason(r WaitingReason) bool {
	return r == WaitingReasonRunnable || r == WaitingReasonInput
}
