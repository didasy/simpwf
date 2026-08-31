package repository

// orderSQL translates an allowlisted order expression into an ORDER BY
// clause. It returns an empty string for anything not allowlisted.
func orderSQL(order string, allowed map[string]bool) string {
	field := order
	desc := false
	if len(order) > 0 && order[0] == '-' {
		field = order[1:]
		desc = true
	}
	if !allowed[field] || field == "" {
		return ""
	}
	dir := "ASC"
	if desc {
		dir = "DESC"
	}
	return `"` + field + `" ` + dir
}
