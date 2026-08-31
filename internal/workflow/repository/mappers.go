package repository

import (
	"encoding/json"
	"time"

	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"gorm.io/datatypes"
)

// jsonCol converts a raw JSON field for persistence, falling back to a
// literal when the field is unset so not-null JSONB columns never receive
// SQL NULL.
func jsonCol(raw json.RawMessage, fallback string) datatypes.JSON {
	if len(raw) == 0 {
		return datatypes.JSON(fallback)
	}
	return datatypes.JSON(raw)
}

// UserToModel maps a domain user to its persistence model.
func UserToModel(u model.User) UserModel {
	return UserModel{
		ID:        u.ID,
		Name:      u.Name,
		Email:     u.Email,
		Metadata:  jsonCol(u.Metadata, "{}"),
		CreatedAt: u.CreatedAt,
		UpdatedAt: u.UpdatedAt,
	}
}

// UserFromModel maps a persistence user back to the domain.
func UserFromModel(m UserModel) model.User {
	return model.User{
		ID:        m.ID,
		Name:      m.Name,
		Email:     m.Email,
		Metadata:  json.RawMessage(m.Metadata),
		CreatedAt: m.CreatedAt,
		UpdatedAt: m.UpdatedAt,
	}
}

// NodeDefinitionToModel maps a domain node definition to its persistence model.
func NodeDefinitionToModel(n model.NodeDefinition) NodeDefinitionModel {
	return NodeDefinitionModel{
		ID:                n.ID,
		Name:              n.Name,
		Version:           n.Version,
		PreviousVersionID: n.PreviousVersionID,
		LineageID:         n.LineageID,
		Type:              n.Type,
		Content:           datatypes.JSON(n.Content),
		CreatedBy:         n.CreatedBy,
		UpdatedBy:         n.UpdatedBy,
		CreatedAt:         n.CreatedAt,
		UpdatedAt:         n.UpdatedAt,
	}
}

// NodeDefinitionFromModel maps a persistence node definition back to the domain.
func NodeDefinitionFromModel(m NodeDefinitionModel) model.NodeDefinition {
	return model.NodeDefinition{
		ID:                m.ID,
		Name:              m.Name,
		Version:           m.Version,
		PreviousVersionID: m.PreviousVersionID,
		LineageID:         m.LineageID,
		Type:              m.Type,
		Content:           json.RawMessage(m.Content),
		CreatedBy:         m.CreatedBy,
		UpdatedBy:         m.UpdatedBy,
		CreatedAt:         m.CreatedAt,
		UpdatedAt:         m.UpdatedAt,
	}
}

// WorkflowDefinitionToModel maps a domain workflow definition to persistence.
func WorkflowDefinitionToModel(w model.WorkflowDefinition) WorkflowDefinitionModel {
	return WorkflowDefinitionModel{
		ID:                w.ID,
		Name:              w.Name,
		Version:           w.Version,
		PreviousVersionID: w.PreviousVersionID,
		LineageID:         w.LineageID,
		Content:           datatypes.JSON(w.Content),
		CreatedBy:         w.CreatedBy,
		UpdatedBy:         w.UpdatedBy,
		CreatedAt:         w.CreatedAt,
		UpdatedAt:         w.UpdatedAt,
	}
}

// WorkflowDefinitionFromModel maps a persistence workflow definition back.
func WorkflowDefinitionFromModel(m WorkflowDefinitionModel) model.WorkflowDefinition {
	return model.WorkflowDefinition{
		ID:                m.ID,
		Name:              m.Name,
		Version:           m.Version,
		PreviousVersionID: m.PreviousVersionID,
		LineageID:         m.LineageID,
		Content:           json.RawMessage(m.Content),
		CreatedBy:         m.CreatedBy,
		UpdatedBy:         m.UpdatedBy,
		CreatedAt:         m.CreatedAt,
		UpdatedAt:         m.UpdatedAt,
	}
}

// WorkflowRequestToModel maps a domain request to persistence.
func WorkflowRequestToModel(r model.WorkflowRequest) WorkflowRequestModel {
	return WorkflowRequestModel{
		ID:                   r.ID,
		WorkflowDefinitionID: r.WorkflowDefinitionID,
		Context:              jsonCol(r.Context, "{}"),
		CreatedBy:            r.CreatedBy,
		CreatedAt:            r.CreatedAt,
	}
}

// WorkflowRequestFromModel maps a persistence request back to the domain.
func WorkflowRequestFromModel(m WorkflowRequestModel) model.WorkflowRequest {
	return model.WorkflowRequest{
		ID:                   m.ID,
		WorkflowDefinitionID: m.WorkflowDefinitionID,
		Context:              json.RawMessage(m.Context),
		CreatedBy:            m.CreatedBy,
		CreatedAt:            m.CreatedAt,
	}
}

// WorkflowInstanceToModel maps a domain instance to persistence.
func WorkflowInstanceToModel(w model.WorkflowInstance) WorkflowInstanceModel {
	return WorkflowInstanceModel{
		ID:                   w.ID,
		WorkflowDefinitionID: w.WorkflowDefinitionID,
		Status:               string(w.Status),
		WaitingReason:        string(w.WaitingReason),
		PauseRequested:       w.PauseRequested,
		TerminationPending:   w.TerminationPending,
		CurrentGroupID:       uuidPtr(w.CurrentGroupID),
		CurrentNodeID:        uuidPtr(w.CurrentNodeID),
		Frame:                jsonCol(w.Frame, "{}"),
		Context:              jsonCol(w.Context, "{}"),
		Counters:             jsonCol(w.Counters, "{}"),
		Revision:             w.Revision,
		LeasedBy:             w.LeasedBy,
		LeaseExpiry:          timePtr(w.LeaseExpiry),
		Error:                w.Error,
		StartedAt:            w.StartedAt,
		FinishedAt:           w.FinishedAt,
		CreatedBy:            w.CreatedBy,
		UpdatedBy:            w.UpdatedBy,
		CreatedAt:            w.CreatedAt,
		UpdatedAt:            w.UpdatedAt,
	}
}

// WorkflowInstanceFromModel maps a persistence instance back to the domain.
func WorkflowInstanceFromModel(m WorkflowInstanceModel) model.WorkflowInstance {
	return model.WorkflowInstance{
		ID:                   m.ID,
		WorkflowDefinitionID: m.WorkflowDefinitionID,
		Status:               model.WorkflowStatus(m.Status),
		WaitingReason:        model.WaitingReason(m.WaitingReason),
		PauseRequested:       m.PauseRequested,
		TerminationPending:   m.TerminationPending,
		CurrentGroupID:       strVal(m.CurrentGroupID),
		CurrentNodeID:        strVal(m.CurrentNodeID),
		Frame:                json.RawMessage(m.Frame),
		Context:              json.RawMessage(m.Context),
		Counters:             json.RawMessage(m.Counters),
		Revision:             m.Revision,
		LeasedBy:             m.LeasedBy,
		LeaseExpiry:          timeVal(m.LeaseExpiry),
		Error:                m.Error,
		StartedAt:            m.StartedAt,
		FinishedAt:           m.FinishedAt,
		CreatedBy:            m.CreatedBy,
		UpdatedBy:            m.UpdatedBy,
		CreatedAt:            m.CreatedAt,
		UpdatedAt:            m.UpdatedAt,
	}
}

// timeVal dereferences a nullable timestamp, defaulting to the zero time.
func timeVal(p *time.Time) time.Time {
	if p == nil {
		return time.Time{}
	}
	return *p
}

// uuidPtr converts a non-empty id to a nullable uuid column value.
func uuidPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func strVal(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// timePtr converts a zero time to NULL for nullable timestamp columns.
func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// NodeInstanceToModel maps a domain node instance to persistence.
func NodeInstanceToModel(n model.NodeInstance) NodeInstanceModel {
	return NodeInstanceModel{
		ID:                 n.ID,
		WorkflowInstanceID: n.WorkflowInstanceID,
		NodeID:             n.NodeID,
		NodeDefinitionID:   uuidPtr(n.NodeDefinitionID),
		Name:               n.Name,
		Type:               n.Type,
		Attempt:            n.Attempt,
		Status:             string(n.Status),
		Input:              jsonCol(n.Input, "null"),
		Output:             jsonCol(n.Output, "null"),
		Error:              n.Error,
		ContextBefore:      jsonCol(n.ContextBefore, "null"),
		ContextAfter:       jsonCol(n.ContextAfter, "null"),
		RecoveryPolicy:     n.RecoveryPolicy,
		RecoveryResult:     n.RecoveryResult,
		StartedAt:          n.StartedAt,
		FinishedAt:         n.FinishedAt,
		StoppedAt:          n.StoppedAt,
		Cancelled:          n.Cancelled,
		CreatedAt:          n.CreatedAt,
		UpdatedAt:          n.UpdatedAt,
	}
}

// NodeInstanceFromModel maps a persistence node instance back to the domain.
func NodeInstanceFromModel(m NodeInstanceModel) model.NodeInstance {
	return model.NodeInstance{
		ID:                 m.ID,
		WorkflowInstanceID: m.WorkflowInstanceID,
		NodeID:             m.NodeID,
		NodeDefinitionID:   strVal(m.NodeDefinitionID),
		Name:               m.Name,
		Type:               m.Type,
		Attempt:            m.Attempt,
		Status:             model.NodeStatus(m.Status),
		Input:              json.RawMessage(m.Input),
		Output:             json.RawMessage(m.Output),
		Error:              m.Error,
		ContextBefore:      json.RawMessage(m.ContextBefore),
		ContextAfter:       json.RawMessage(m.ContextAfter),
		RecoveryPolicy:     m.RecoveryPolicy,
		RecoveryResult:     m.RecoveryResult,
		StartedAt:          m.StartedAt,
		FinishedAt:         m.FinishedAt,
		StoppedAt:          m.StoppedAt,
		Cancelled:          m.Cancelled,
		CreatedAt:          m.CreatedAt,
		UpdatedAt:          m.UpdatedAt,
	}
}

// WorkflowInstanceEventToModel maps a domain event to persistence.
func WorkflowInstanceEventToModel(e model.WorkflowInstanceEvent) WorkflowInstanceEventModel {
	return WorkflowInstanceEventModel{
		ID:                 e.ID,
		WorkflowInstanceID: e.WorkflowInstanceID,
		Type:               e.Type,
		Data:               jsonCol(e.Data, "{}"),
		CreatedBy:          e.CreatedBy,
		CreatedAt:          e.CreatedAt,
	}
}

// WorkflowInstanceEventFromModel maps a persistence event back to the domain.
func WorkflowInstanceEventFromModel(m WorkflowInstanceEventModel) model.WorkflowInstanceEvent {
	return model.WorkflowInstanceEvent{
		ID:                 m.ID,
		WorkflowInstanceID: m.WorkflowInstanceID,
		Type:               m.Type,
		Data:               json.RawMessage(m.Data),
		CreatedBy:          m.CreatedBy,
		CreatedAt:          m.CreatedAt,
	}
}

// InputDeliveryToModel maps a domain delivery to persistence.
func InputDeliveryToModel(d model.InputDelivery) InputDeliveryModel {
	return InputDeliveryModel{
		ID:                 d.ID,
		WorkflowInstanceID: d.WorkflowInstanceID,
		NodeInstanceID:     d.NodeInstanceID,
		IdempotencyKey:     d.IdempotencyKey,
		Payload:            jsonCol(d.Payload, "{}"),
		Accepted:           d.Accepted,
		Error:              d.Error,
		CreatedAt:          d.CreatedAt,
	}
}

// InputDeliveryFromModel maps a persistence delivery back to the domain.
func InputDeliveryFromModel(m InputDeliveryModel) model.InputDelivery {
	return model.InputDelivery{
		ID:                 m.ID,
		WorkflowInstanceID: m.WorkflowInstanceID,
		NodeInstanceID:     m.NodeInstanceID,
		IdempotencyKey:     m.IdempotencyKey,
		Payload:            json.RawMessage(m.Payload),
		Accepted:           m.Accepted,
		Error:              m.Error,
		CreatedAt:          m.CreatedAt,
	}
}
