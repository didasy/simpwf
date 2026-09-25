// Package all bundles every shipped custom node type. It exists solely so
// cmd/app/main.go needs one blank import; leaf node packages self-register
// in init. Adding a new custom node = new subpackage under
// pkg/customnode/<name> + one blank-import line here.
package all

import (
	_ "github.com/simpwf/workflow-engine/pkg/customnode/jev"
	_ "github.com/simpwf/workflow-engine/pkg/customnode/s3fetch"
)
