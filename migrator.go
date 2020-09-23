package clickhouse

import (
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Migrator struct {
	migrator.Migrator
	Dialector
}

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
		return fmt.Errorf("failed to create index with name %v", name)
	})
}

func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	return fmt.Errorf("renaming index is not supported in clickhouse")
}

func (m Migrator) DropIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}
		return m.DB.Exec("DROP INDEX ?", clause.Column{Name: name}).Error
	})
}
