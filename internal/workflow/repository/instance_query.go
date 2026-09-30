package repository

// InstanceListQuery is the parsed list/filter/pagination query for workflow
// instance lists. Order must be one of the allowlisted fields with an
// optional leading "-" for descending; callers validate before use.
type InstanceListQuery struct {
	Page                 int
	PerPage              int
	Order                string
	IDs                  []string
	WorkflowDefinitionID string
	Statuses             []string
	// CreatedBy scopes the listing to one owner. The service sets it from the
	// authenticated principal, so a caller cannot widen the result set by
	// naming another user: the field is not parsed from the query string.
	CreatedBy string
}

var instanceOrderFields = map[string]bool{
	"id": true, "workflow_definition_id": true, "status": true,
	"created_at": true, "updated_at": true,
}

// instanceOrderSQL translates an allowlisted order expression into an
// ORDER BY clause. It returns an empty string for anything not allowlisted.
func instanceOrderSQL(order string) string {
	return orderSQL(order, instanceOrderFields)
}
