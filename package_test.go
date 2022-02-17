package sqlair

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func setupDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	assert.Nil(t, err)
	return db
}

func runTx(t *testing.T, db *sql.DB, fn func(*sql.Tx) error) {
	tx, err := db.Begin()
	assert.Nil(t, err)

	if err := fn(tx); err != nil {
		tx.Rollback()
		assert.Nil(t, err)
	}

	err = tx.Commit()
	assert.Nil(t, err)
}
