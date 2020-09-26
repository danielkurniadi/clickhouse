package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/go-version"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"

	_ "github.com/ClickHouse/clickhouse-go"
)

type Config struct {
	DriverName               string
	DSN                      string
	Conn                     gorm.ConnPool
	DisableDatetimePrecision bool
	DontSupportRenameColumn  bool
}

type Dialector struct {
	*Config
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{Config: &Config{DSN: dsn}}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (dialector Dialector) Name() string {
	return "clickhouse"
}

func (dialector Dialector) Initialize(db *gorm.DB) (err error) {
	// register callbacks
	ctx := context.Background()
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{})
	db.Callback().
		Update().
		Replace("gorm:update", func(db *gorm.DB) { return }) // TODO (iqdf) Replace func

	// assign option fields to default values
	if dialector.DriverName == "" {
		dialector.DriverName = "clickhouse"
	}
	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else {
		db.ConnPool, err = sql.Open(dialector.DriverName, dialector.DSN)
		if err != nil {
			return err
		}
	}
	var vs string
	err = db.ConnPool.QueryRowContext(ctx, "SELECT version()").Scan(&vs)
	if err != nil {
		return err
	}
	dbversion, _ := version.NewVersion(vs)
	versionNoRenameColumn, _ := version.NewConstraint("< 20.4")

	if versionNoRenameColumn.Check(dbversion) {
		dialector.Config.DontSupportRenameColumn = true
	}
	return
}

func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{
		Migrator: migrator.Migrator{
			Config: migrator.Config{
				DB:        db,
				Dialector: dialector,
			},
		},
		Dialector: dialector,
	}
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "boolean"
	case schema.Int, schema.Uint:
		sqlType := "Int64"
		switch {
		case field.Size <= 8:
			sqlType = "Int8"
		case field.Size <= 16:
			sqlType = "Int16"
		case field.Size <= 32:
			sqlType = "Int32"
		}
		if field.DataType == schema.Uint {
			sqlType = "U" + sqlType
		}
		return sqlType
	case schema.Float:
		if field.Precision > 0 {
			return fmt.Sprintf("decimal(%d, %d)", field.Precision, field.Scale)
		}
		if field.Size <= 32 {
			return "Float32"
		}
		return "Float64"
	case schema.String:
		if field.Size == 0 {
			return "String"
		}
		return fmt.Sprintf("FixedString(%d)", field.Size)
	case schema.Bytes:
		return "String"
	case schema.Time:
		// TODO: support TimeZone
		precision := ""
		if !dialector.DisableDatetimePrecision {
			if field.Precision == 0 {
				field.Precision = 3
			}
			if field.Precision > 0 {
				precision = fmt.Sprintf("(%d)", field.Precision)
			}
		}
		return "DateTime64" + precision
	}
	return string(field.DataType)
}

func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('`')
	if strings.Contains(str, ".") {
		for idx, str := range strings.Split(str, ".") {
			if idx > 0 {
				writer.WriteString(".`")
			}
			writer.WriteString(str)
			writer.WriteByte('`')
		}
	} else {
		writer.WriteString(str)
		writer.WriteByte('`')
	}
}

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `"`, vars...)
}

func (dialectopr Dialector) SavePoint(tx *gorm.DB, name string) error {
	tx.Exec("SAVEPOINT " + name)
	return nil
}

func (dialectopr Dialector) RollbackTo(tx *gorm.DB, name string) error {
	tx.Exec("ROLLBACK TO SAVEPOINT " + name)
	return nil
}
