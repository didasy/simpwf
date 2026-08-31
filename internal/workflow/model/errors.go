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
var (
	ErrNotFound      = errors.New("model: not found")
	ErrConflict      = errors.New("model: conflict")
	ErrInvalid       = errors.New("model: invalid")
	ErrTerminalState = errors.New("model: terminal state")
)
