# GORM ClickHouse Driver

clickhouse support for GORM

## Quick Start

You can simply test your connection to your database with the following:

```go
import (
  "gorm.io/driver/clickhouse"
  "gorm.io/gorm"
)

func main() {
  dsn := "tcp://localhost:9000?database=gorm&username=gorm&password=gorm&read_timeout=10&write_timeout=20"
  db, err := gorm.Open(clickhouse.Open(dsn), &gorm.Config{})
  if err != nil {
    panic(err)
  }

  // do something with db
  db.Create(&user)
  db.Find(&user, "id = ?", 10)
  // ...
}
```

## Advanced Configuration

```go
import (
  "gorm.io/driver/clickhouse"
  "gorm.io/gorm"
)

// refer to https://github.com/ClickHouse/clickhouse-go
var dsn = "tcp://localhost:9000?database=gorm&username=gorm&password=gorm&read_timeout=10&write_timeout=20"

func main() {
  db, err := gorm.Open(clickhouse.New(click.Config{
    DSN: dsn,
    Conn: conn,                       // initialize with existing database conn
    DisableDatetimePrecision: true,   // disable datetime64 precision, not supported before clickhouse 20.4
    DontSupportRenameColumn: true,    // rename column not supported before clickhouse 20.4
    SkipInitializeWithVersion: false, // smart configure based on used version
  }), &gorm.Config{})
}
```

Checkout [https://gorm.io](https://gorm.io) for details.
