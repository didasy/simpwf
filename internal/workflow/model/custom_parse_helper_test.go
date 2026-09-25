package model_test

import "errors"

func errNew(s string) error { return errors.New(s) }
