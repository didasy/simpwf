package model

import "errors"

// Domain error sentinels. Services wrap them with context
// (fmt.Errorf("...: %w", model.ErrNotFound)); handlers map them to HTTP
// status codes:
//
//	ErrNotFound      -> 404
//	ErrConflict      -> 409
//	ErrInvalid       -> 422 (request body) or 400 (query)
//	ErrTerminalState -> 409
//	ErrForbidden     -> 403
var (
	ErrNotFound      = errors.New("model: not found")
	ErrConflict      = errors.New("model: conflict")
	ErrInvalid       = errors.New("model: invalid")
	ErrTerminalState = errors.New("model: terminal state")
	// ErrForbidden is a valid credential that failed an authorization
	// gate. It is distinct from a missing or invalid credential, which is
	// a 401: the caller is known, the request is simply not allowed.
	ErrForbidden = errors.New("model: forbidden")
)
