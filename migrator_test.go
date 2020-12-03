package clickhouse_test

import (
	"math/rand"
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

func TestMigrate(t *testing.T) {
	allModels := []interface{}{&User{}}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(allModels), func(i, j int) { allModels[i], allModels[j] = allModels[j], allModels[i] })

	DB.Migrator().DropTable("user_speaks", "user_friends")

	if err := DB.Migrator().DropTable(allModels...); err != nil {
		t.Fatalf("Failed to drop table, got error %v", err)
	}

	if err := DB.AutoMigrate(allModels...); err != nil {
		t.Fatalf("Failed to auto migrate, but got error %v", err)
	}

	for _, m := range allModels {
		if !DB.Migrator().HasTable(m) {
			t.Fatalf("Failed to create table for %#v", m)
		}
	}
}
