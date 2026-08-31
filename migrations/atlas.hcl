# Atlas configuration for the workflow engine.
#
# The desired schema is produced by cmd/atlas-loader (Go Program Mode) from
# the GORM persistence models. Versioned SQL lives in migrations/versions and
# its checksum in migrations/versions/atlas.sum.

variable "dev_url" {
  type    = string
  default = "docker://postgres/16/dev"
}

data "external_schema" "gorm" {
  program = [
    "go", "run", "./cmd/atlas-loader",
  ]
}

env "gorm" {
  src = data.external_schema.gorm.url
  # dev is a scratch database; Atlas uses it to simulate migrations.
  # Override for local setups: --var dev_url="postgres://.../dev?sslmode=disable"
  dev = var.dev_url
  migration {
    dir = "file://migrations/versions"
  }
  format {
    migrate {
      diff = "{{ sql . \"  \" }}"
    }
  }
}
