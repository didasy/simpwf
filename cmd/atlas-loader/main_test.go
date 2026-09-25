package main

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestMainEmitsSecretsTable(t *testing.T) {
	originalStdout := os.Stdout
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = write
	t.Cleanup(func() { os.Stdout = originalStdout })

	main()

	if err := write.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(read)
	if err != nil {
		t.Fatal(err)
	}

	ddl := string(output)
	if !strings.Contains(ddl, `CREATE TABLE "secrets"`) {
		t.Fatal("atlas loader output does not create secrets table")
	}
	if !strings.Contains(ddl, `"key" text,`) || !strings.Contains(ddl, `PRIMARY KEY ("key")`) {
		t.Fatal("atlas loader output does not define secrets.key as text primary key")
	}
}
