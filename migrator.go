package clickhouse

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Migrator struct {
	migrator.Migrator
	Dialector
}

// Errors enumeration
var (
	ErrRenameIndexUnsupported = errors.New("renaming index is not supported in clickhouse")
	ErrCreateIndexFailed      = errors.New("failed to create index with name")
)

// Database

func (m Migrator) CurrentDatabase() (name string) {
	m.DB.Raw("SELECT currentDatabase()").Row().Scan(&name)
	return
}

func (m Migrator) FullDataTypeOf(field *schema.Field) clause.Expr {
	expr := m.Migrator.FullDataTypeOf(field)
	if value, ok := field.TagSettings["COMMENT"]; ok {
		expr.SQL += "COMMENT" + m.Dialector.Explain("?", value)
	}
	return expr
}

// Tables

func (m Migrator) CreateTable(models ...interface{}) error {
	for _, model := range m.ReorderModels(models, false) {
		tx := m.DB.Session(new(gorm.Session))
		if err := m.RunWithValue(model, func(stmt *gorm.Statement) (err error) {
			var (
				createTableSQL = "CREATE TABLE ? (%s %s %s) ENGINE=%s"
				args           = []interface{}{clause.Table{Name: stmt.Table}}
			)

			columnSlice := make([]string, 0, len(stmt.Schema.DBNames))
			for _, dbName := range stmt.Schema.DBNames {
				field := stmt.Schema.FieldsByDBName[dbName]
				columnSlice = append(columnSlice, "? ?")
				args = append(args, clause.Column{Name: dbName}, m.DB.Migrator().FullDataTypeOf(field))
			}
			columnStr := strings.Join(columnSlice, ",")

			constrSlice := make([]string, 0, len(columnSlice))
			for _, check := range stmt.Schema.ParseCheckConstraints() {
				constrSlice = append(constrSlice, "CONSTRAINT ? CHECK ?")
				args = append(args, clause.Column{Name: check.Name}, clause.Expr{SQL: check.Constraint})
			}
			constrStr := strings.Join(constrSlice, ",")

			indexSlice := make([]string, 0, 10)
			for _, index := range stmt.Schema.ParseIndexes() {
				if m.CreateIndexAfterCreateTable {
					defer func(model interface{}, indexName string) {
						err = tx.Migrator().CreateIndex(model, indexName)
					}(model, index.Name)
					continue
				}
				str := "INDEX ? ?"
				if index.Class != "" {
					str = index.Class + " " + str
				}
				indexSlice = append(indexSlice, str)

				buildIndexOptions := tx.Migrator().(migrator.BuildIndexOptionsInterface)
				indexOptions := buildIndexOptions.BuildIndexOptions(index.Fields, stmt)
				args = append(args, clause.Expr{SQL: index.Name}, indexOptions)
			}
			indexStr := strings.Join(indexSlice, ",")

			// Finally assemble CREATE TABLE ... SQL string consisting of
			// column names string, constraint/check string, indexes name string
			// followed by engine options
			createTableSQL = fmt.Sprintf(createTableSQL, columnStr, constrStr, indexStr, "MergeTree()")
			return
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m Migrator) DropTable(dsts ...interface{}) error {
	return nil
}

func (m Migrator) HasTable(dst interface{}) bool {
	return false
}

func (m Migrator) RenameTable(oldName, newName interface{}) error {
	return nil
}

// Columns

func (m Migrator) AddColumn(dst interface{}, field string) error {
	return nil
}

func (m Migrator) DropColumn(dst interface{}, field string) error {
	return nil
}

func (m Migrator) AlterColumn(value interface{}, field string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(field); field != nil {
			return m.DB.Exec(
				"ALTER TABLE ? MODIFY COLUMN ? ?",
				clause.Table{Name: stmt.Table},
				clause.Column{Name: field.DBName},
				m.FullDataTypeOf(field),
			).Error
		}
		return fmt.Errorf("failed to look up field with name: %s", field)
	})
}

func (m Migrator) MigrateColumn(dst interface{}, field *schema.Field, columnType *sql.ColumnType) error {
	return nil
}

func (m Migrator) HasColumn(dst interface{}, field string) bool {
	return false
}

// NOTE: Only supported after ClickHouse 20.4 and above. Before v20.4,
// renaming column implementation is not trivial, performing rename column
// without reworking of ALTERs will cause race conditions in replicated tables
func (m Migrator) RenameColumn(value interface{}, oldName, newName string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if m.Dialector.DontSupportRenameColumn {
			var field *schema.Field
			if f := stmt.Schema.LookUpField(oldName); f != nil {
				oldName = f.DBName
				field = f
			}
			if f := stmt.Schema.LookUpField(newName); f != nil {
				newName = f.DBName
				field = f
			}
			if field != nil {
				return m.DB.Exec(
					"ALTER TABLE ? RENAME COLUMN ? TO ?",
					clause.Table{Name: stmt.Table},
					clause.Column{Name: oldName},
					clause.Column{Name: newName},
				).Error
			}
			return fmt.Errorf("failed to look up field with name: %s", newName)
		}
		return fmt.Errorf(
			"failed to rename column. renaming column is disabled or isn't supported")
	})
}

func (m Migrator) ColumnTypes(dst interface{}) ([]*sql.ColumnType, error) {
	return []*sql.ColumnType{}, nil
}

// Indexes

// Table Index: https://clickhouse.tech/docs/en/sql-reference/statements/alter/index/
func (m Migrator) BuildIndexOptions(opts []schema.IndexOption, stmt *gorm.Statement) (results []interface{}) {
	// TODO (iqdf): // type value name
	for _, opt := range opts {
		str := stmt.Quote(opt.DBName)
		if opt.Expression != "" {
			str = opt.Expression
		}
		results = append(results, clause.Expr{SQL: str})
	}
	return
}

// Table Index: https://clickhouse.tech/docs/en/sql-reference/statements/alter/index/
func (m Migrator) CreateIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			opts := m.BuildIndexOptions(idx.Fields, stmt)
			values := []interface{}{
				clause.Table{Name: stmt.Table},
				clause.Column{Name: idx.Name},
				opts,
			}

			// NOTE: concept of UNIQUE | FULLTEXT | SPATIAL index
			// is not supported in clickhouse
			createIndexSQL := "ALTER TABLE ? ADD INDEX ? ? TYPE %s GRANULARITY %d" // TODO (iqdf) how to inject Granularity
			createIndexSQL = fmt.Sprintf(createIndexSQL, idx.Type, 0)              // Granularity: 0
			return m.DB.Exec(createIndexSQL, values...).Error
		}
		return ErrCreateIndexFailed
	})
}

func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	return ErrRenameIndexUnsupported
}

func (m Migrator) DropIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}
		return m.DB.Exec("DROP INDEX ?", clause.Column{Name: name}).Error
	})
}
