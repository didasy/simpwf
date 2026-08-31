package handler

import (
	"crypto/subtle"
	"net/http"

	"github.com/gin-gonic/gin"
)

// APIKeyHeader is the header expected when API token auth is enabled.
const APIKeyHeader = "X-Api-Token"

// RequireAuth returns Gin middleware that enforces api-token authentication
// via the X-Api-Token header (constant-time comparison).
func RequireAuth(expectedToken string) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.GetHeader(APIKeyHeader)
		if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(expectedToken)) != 1 {
			WriteProblem(c, http.StatusUnauthorized, "missing or invalid API token")
			return
		}
		c.Next()
	}
}
