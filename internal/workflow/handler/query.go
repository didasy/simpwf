package handler

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/simpwf/workflow-engine/internal/workflow/model"
	"github.com/simpwf/workflow-engine/pkg/ids"
)

const (
	maxPerPage     = 200
	defaultPerPage = 50
	maxIDFilters   = 100
)

// ListKind selects the order/type allowlist for a definition list endpoint.
type ListKind int

const (
	// ListKindWorkflow is the workflow definition list.
	ListKindWorkflow ListKind = iota
	// ListKindNode is the node definition list.
	ListKindNode
	// ListKindInstance is the workflow instance list.
	ListKindInstance
)

var workflowOrderFields = map[string]bool{
	"id": true, "name": true, "version": true, "lineage_id": true,
	"created_at": true, "updated_at": true,
}

var nodeOrderFields = map[string]bool{
	"id": true, "name": true, "version": true, "lineage_id": true,
	"type": true, "created_at": true, "updated_at": true,
}

var instanceOrderFields = map[string]bool{
	"id": true, "workflow_definition_id": true, "status": true,
	"created_at": true, "updated_at": true,
}

// ListQuery is a parsed definition list query.
type ListQuery struct {
	Page       int
	PerPage    int
	Order      string
	IDs        []string
	Name       string
	LineageID  string
	Version    *int
	LatestOnly bool
	Type       string
}

// ParseListQuery parses and validates a definition list query.
func ParseListQuery(c *gin.Context, kind ListKind) (*ListQuery, error) {
	q := &ListQuery{Page: 1, PerPage: defaultPerPage, Order: "-created_at"}

	if v := c.Query("page"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("query: page must be an integer >= 1, got %q", v)
		}
		q.Page = n
	}
	if v := c.Query("per_page"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxPerPage {
			return nil, fmt.Errorf("query: per_page must be an integer in [1,%d], got %q", maxPerPage, v)
		}
		q.PerPage = n
	}
	if v := c.Query("order"); v != "" {
		if !validOrder(v, kind) {
			return nil, fmt.Errorf("query: order %q is not allowlisted", v)
		}
		q.Order = v
	}

	idsRaw, ok := c.GetQueryArray("id")
	if ok {
		if len(idsRaw) > maxIDFilters {
			return nil, fmt.Errorf("query: at most %d id filters allowed", maxIDFilters)
		}
		for _, id := range idsRaw {
			if !ids.Valid(id) {
				return nil, fmt.Errorf("query: id %q is not a valid uuid", id)
			}
			q.IDs = append(q.IDs, id)
		}
	}

	q.Name = c.Query("name")
	if v := c.Query("lineage_id"); v != "" {
		if !ids.Valid(v) {
			return nil, fmt.Errorf("query: lineage_id %q is not a valid uuid", v)
		}
		q.LineageID = v
	}
	if v := c.Query("version"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("query: version must be an integer >= 1, got %q", v)
		}
		q.Version = &n
	}
	if v := c.Query("latest_only"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return nil, fmt.Errorf("query: latest_only must be a boolean, got %q", v)
		}
		q.LatestOnly = b
	}
	if v := c.Query("type"); v != "" {
		if kind != ListKindNode {
			return nil, errors.New("query: type filter is only valid for node definitions")
		}
		q.Type = v
	}
	return q, nil
}

func validOrder(order string, kind ListKind) bool {
	field := strings.TrimPrefix(order, "-")
	if field == order && strings.HasPrefix(order, "-") {
		return false // "--created_at" style
	}
	allow := workflowOrderFields
	switch kind {
	case ListKindNode:
		allow = nodeOrderFields
	case ListKindInstance:
		allow = instanceOrderFields
	}
	return allow[field]
}

// InstanceListQuery is a parsed workflow instance list query.
type InstanceListQuery struct {
	Page                 int
	PerPage              int
	Order                string
	IDs                  []string
	WorkflowDefinitionID string
	Statuses             []string
}

// ParseInstanceListQuery parses and validates a workflow instance list query.
func ParseInstanceListQuery(c *gin.Context) (*InstanceListQuery, error) {
	q := &InstanceListQuery{Page: 1, PerPage: defaultPerPage, Order: "-created_at"}

	if v := c.Query("page"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("query: page must be an integer >= 1, got %q", v)
		}
		q.Page = n
	}
	if v := c.Query("per_page"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxPerPage {
			return nil, fmt.Errorf("query: per_page must be an integer in [1,%d], got %q", maxPerPage, v)
		}
		q.PerPage = n
	}
	if v := c.Query("order"); v != "" {
		if !validOrder(v, ListKindInstance) {
			return nil, fmt.Errorf("query: order %q is not allowlisted", v)
		}
		q.Order = v
	}

	idsRaw, ok := c.GetQueryArray("id")
	if ok {
		if len(idsRaw) > maxIDFilters {
			return nil, fmt.Errorf("query: at most %d id filters allowed", maxIDFilters)
		}
		for _, id := range idsRaw {
			if !ids.Valid(id) {
				return nil, fmt.Errorf("query: id %q is not a valid uuid", id)
			}
			q.IDs = append(q.IDs, id)
		}
	}
	if v := c.Query("workflow_definition_id"); v != "" {
		if !ids.Valid(v) {
			return nil, fmt.Errorf("query: workflow_definition_id %q is not a valid uuid", v)
		}
		q.WorkflowDefinitionID = v
	}
	statusesRaw, ok := c.GetQueryArray("status")
	if ok {
		for _, s := range statusesRaw {
			if !model.ValidWorkflowStatus(model.WorkflowStatus(s)) {
				return nil, fmt.Errorf("query: status %q is not a valid workflow status", s)
			}
			q.Statuses = append(q.Statuses, s)
		}
	}
	return q, nil
}
