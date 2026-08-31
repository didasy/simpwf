package repository

// DefinitionListQuery is the parsed list/filter/pagination query for
// definition lists. Order must be one of the allowlisted fields with an
// optional leading "-" for descending; callers validate before use.
type DefinitionListQuery struct {
	Page       int
	PerPage    int
	Order      string
	IDs        []string
	Name       string
	LineageID  string
	Version    *int
	LatestOnly bool
	Type       string // node definitions only
}

var definitionOrderFields = map[string]bool{
	"id": true, "name": true, "version": true, "lineage_id": true,
	"type": true, "created_at": true, "updated_at": true,
}

// definitionOrderSQL translates an allowlisted order expression into an
// ORDER BY clause. It returns an empty string for anything not allowlisted.
func definitionOrderSQL(order string) string {
	return orderSQL(order, definitionOrderFields)
}
