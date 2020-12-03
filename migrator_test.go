package clickhouse_test

import (
	"time"
)

type User struct {
	ID        uint
	Name      string
	FirstName string
	LastName  string
	Age       int64
	CreatedAt time.Time
	UpdatedAt time.Time
}
