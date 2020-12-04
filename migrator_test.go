package clickhouse_test

import (
	"testing"
	"time"
)

type User struct {
	ID        uint
	Name      string
	FirstName string
	LastName  string
	Age       int64
	Active    bool
	Salary    float32
	CreatedAt time.Time
	UpdatedAt time.Time
}

func TestAutoMigrate(t *testing.T) {
	type UserMigrateColumn struct {
		ID       uint
		Name     string
		IsAdmin  bool
		Birthday time.Time `gorm:"precision:4"`
	}

	if DB.Migrator().HasColumn("users", "is_admin") {
		t.Fatalf("users's is_admin column should not exists")
	}

	if err := DB.Table("users").AutoMigrate(&UserMigrateColumn{}); err != nil {
		t.Fatalf("no error should happen when auto migrate")
	}

	if !DB.Migrator().HasTable("users") {
		t.Fatalf("users should exists")
	}

	if !DB.Migrator().HasColumn("users", "is_admin") {
		t.Fatalf("users's is_admin column should exists after auto migrate")
	}
}
