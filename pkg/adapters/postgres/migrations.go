package postgres

import (
	"embed"
	"io/fs"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// MigrationsFS returns an fs.FS rooted at this adapter's SQL migration files.
func MigrationsFS() (fs.FS, error) {
	return fs.Sub(migrationsFS, "migrations")
}
