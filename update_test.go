package clickhouse_test

import (
	"testing"
	"time"

	"gorm.io/gorm/utils/tests"
)

func TestUpdate(t *testing.T) {
	var user = User{ID: 3, Name: "update", FirstName: "zhang", LastName: "jinzhu", Age: 18, Active: true, Salary: 8.8888}

	if err := DB.Create(&user).Error; err != nil {
		t.Fatalf("failed to create user, got error %v", err)
	}

	var result User
	if err := DB.Find(&result, user.ID).Error; err != nil {
		t.Fatalf("failed to query user, got error %v", err)
	}

	tests.AssertEqual(t, result, user)

	if err := DB.Model(&result).Update("name", "update-1").Error; err != nil {
		t.Fatalf("failed to update user, got error %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	var result2 User
	if err := DB.First(&result2, user.ID).Error; err != nil {
		t.Fatalf("failed to query user, got error %v", err)
	}

	user.Name = "update-1"
	tests.AssertEqual(t, result2, user)
}
