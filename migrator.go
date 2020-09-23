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
