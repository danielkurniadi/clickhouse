package clickhouse_test

import (
	"testing"

	"gorm.io/gorm/utils/tests"
)

func TestCreate(t *testing.T) {
	var user = User{Name: "create", FirstName: "zhang", LastName: "jinzhu", Age: 18}

	if err := DB.Create(&user).Error; err != nil {
		t.Fatalf("failed to create user, got error %v", err)
	}

	var result User
	if err := DB.Find(&result, user.ID).Error; err != nil {
		t.Fatalf("failed to query user, got error %v", err)
	}

	tests.AssertEqual(t, result, user)
}
