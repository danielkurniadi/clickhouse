# GORM MySQL Driver

## Quick Start

You can simply test your connection to your database with the following:
```go
import (
  "gorm.io/driver/clickhouse"
  "gorm.io/gorm"
)

func main() {
  dsn := "tcp://localhost:9000?debug=true"
  db, err := gorm.Open(sql.Open(dsn), &gorm.Config{})
  if err != nil {
    panic(err)
  }
  // do something with db
  // ...
}
```


## Configuration


```go
import (
  "gorm.io/driver/clickhouse"
  "gorm.io/gorm"
)

// refer to https://github.com/ClickHouse/clickhouse-go
const DSN = "tcp://localhost:9000?username=default&password=password&read_timeout=10&write_timeout=20"

func main() {
db, err := gorm.Open(clickhouse.New(mysql.Config{
    DSN: DSN, 
    DisableDatetimePrecision: true,   // disable datetime64 precision, not supported before clickhouse 20.4
    DontSupportRenameColumn: true,    // rename column not supported before clickhouse 20.4
    SkipInitializeWithVersion: false, // smart configure based on used version
}), &gorm.Config{})
```

## Customized Driver

Customised driver is not supported. Currently there is only one driver that is tested against which is `"clickhouse"` driver from [ClickHouse/clickhouse-go](https://github.com/ClickHouse/clickhouse-go)


Checkout [https://gorm.io](https://gorm.io) for details.
