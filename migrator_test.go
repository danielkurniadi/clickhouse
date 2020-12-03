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
	CreatedAt time.Time
	UpdatedAt time.Time
}

func TestAutoMigrate(t *testing.T) {
	type UserMigrateColumn struct {
		ID       uint
		Name     string
		Salary   float64
		Birthday time.Time `gorm:"precision:4"`
	}

	if DB.Migrator().HasColumn("users", "salary") {
		t.Fatalf("users's salary column should not exists")
	}

	if err := DB.Table("users").AutoMigrate(&UserMigrateColumn{}); err != nil {
		t.Fatalf("no error should happen when auto migrate")
	}

	if !DB.Migrator().HasTable("users") {
		t.Fatalf("users should exists")
	}

	if !DB.Migrator().HasColumn("users", "salary") {
		t.Fatalf("users's salary column should exists after auto migrate")
	}
}
