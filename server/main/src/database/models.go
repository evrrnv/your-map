package database

import (
	"database/sql"
	"sync"

	"github.com/evrrnv/your-map/server/main/src/logging"
	_ "github.com/mattn/go-sqlite3"
)

var DataFolder = "."

type Database struct {
	name     string
	family   string
	db       *sql.DB
	logger   *logging.SeelogWrapper
	isClosed bool
}

type DatabaseLock struct {
	Locked map[string]bool
	sync.RWMutex
}

var databaseLock *DatabaseLock

func init() {
	databaseLock = new(DatabaseLock)
	databaseLock.Lock()
	defer databaseLock.Unlock()
	databaseLock.Locked = make(map[string]bool)
}
