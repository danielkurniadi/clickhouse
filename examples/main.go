package main

import (
	"fmt"
	"time"

	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

// SampleOne (`sample_ones`) table has no constraint and no index during create table
type SampleOne struct {
	ID        uint
	Name      string
	Email     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// SampleTwo (`sample_twos`) table has constraint to check but no index during create table
type SampleTwo struct {
	ID    uint
	Name  string `gorm:"check:name_checker,name <> 'jinzhu'"`
	Email string `gorm:"check:email <> 'diku@gmail.com'"`
}

// Employee (`employees`) table demonstrate support for INDEX during create or migrate
// table. In particular we cover the options for index:
// - expression
// - type
// - renaming index
//
// Meanwhile some other options are not supported in clickhouse, hence ignored
// when you add it in the tags:
// - sort
// - unique
// - collate
// - length
// - class
// - comment
// - check
// - where
//
// See the official docs of ClickHouse:
// https://clickhouse.tech/docs/en/sql-reference/statements/alter/index/
type Employee struct {
	Name      string `gorm:"index"`
	FirstName string `gorm:"index:idx_name"`
	LastName  string `gorm:"index:idx_name"`
	Username  string `gorm:"index:,type:minmax,length:10"`
	Password  string `gorm:"index"`
	Age       int64  `gorm:"index"`
	Age2      int64  `gorm:"index:,expression:ABS(age)"`
}

// Employer (`employers`)table demonstrate support for INDEX during create or migrate
// table. In particular we cover the options as described in Employee comment docs.
// Also we demonstrate how we combine index and constraint checks in one table
// creation at one go.
//
// NOTE that some of the index options here you will see below is not supported by clickhouse
// so nothing happened if you add the index option like unique, check, where, etc.
//
// See comments in Employee (`employees`) table or check official docs of clickhouse
// https://clickhouse.tech/docs/en/sql-reference/statements/alter/index/
type Employer struct {
	Name      string `gorm:"index; check:name_checker,name <> 'jinzhu'"`
	FirstName string `gorm:"index:idx_name,unique; check:concat(first_name, ' ', last_name) <> 'jinzhu zhang'"`
	LastName  string `gorm:"index:idx_name"`
	Username  string `gorm:"index:,type:minmax"`
	Age       int64  `gorm:"index; check:age > 20"`
	WorkYear  int64  `gorm:"index; check:workchecker,work_year > (age + 18)"`
	Comment   string `gorm:"comment:'this is a comment field'"`
}

// DefaultAndComment (`default_and_comments`) table demonstrate support for INDEX
// during create or migrate table. In particular we cover the options for index:
type DefaultAndComment struct {
	DefaultStr    string `gorm:"default:'ThisIsDefaultEyy!'"`
	DefaultNum64  int64  `gorm:"default:199000141"`
	DefaultNumm64 int64  `gorm:"default:toInt64(199000143)"`
	DefaultNum    int    `gorm:"default:1990000142"`
	CommentF      string `gorm:"comment:'this is a comment field ay'"`
	// ExtraDefaultComm string `gorm:"default:'This is Default Ayyyay';comment:'this is a comment field ay'"`
}

// MyIndexedTable (`my_indexed_tables`) table demonstrate more complex indexing expression
type MyIndexedStuff struct {
	U64 uint64 `gorm:"index:a,expression:u64*i32,type:minmax,granularity:3"`
	I32 int32  `gorm:"index:b,experssion:u64*length(ss),type:set(1000),granularity:4"`
	SS  string `gorm:"index"`
}

const DSNf = "tcp://%s:%s?database=%s&username=%s&password=%s&read_timeout=10&write_timeout=20&debug=%t"

func main() {
	var (
		Host  = "localhost"
		Port  = "9000"
		Debug = true

		DBName    = "testdb"
		DBUser    = "default"
		DBPasword = ""
	)

	dsn := fmt.Sprintf(DSNf, Host, Port, DBName, DBUser, DBPasword, Debug)
	conn, err := gorm.Open(clickhouse.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	if err := conn.AutoMigrate(
		&SampleOne{},
		&SampleTwo{},
		&Employee{},
		&Employer{},
		&DefaultAndComment{},
		&MyIndexedStuff{},
	); err != nil {
		fmt.Println("errors?", err)
	}
}
