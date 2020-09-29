package clickhouse

import (
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

// Default values for any SQL options
const (
	DefaultIndexType   = "minmax" // index stores extremes of the expression
	DefaultGranularity = 1        // 1 granule = 8192 rows
)

// Errors enumeration
var (
	ErrRenameColumnUnsupported = errors.New("renaming column is not supported in your clickhouse version < 20.4.")
	ErrRenameIndexUnsupported  = errors.New("renaming index is not supported")
	ErrCreateIndexFailed       = errors.New("failed to create index with name")
)

type Migrator struct {
	migrator.Migrator
	Dialector
}

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

			// Step 1. Build column datatype SQL string
			columnSlice := make([]string, 0, len(stmt.Schema.DBNames))
			for _, dbName := range stmt.Schema.DBNames {
				field := stmt.Schema.FieldsByDBName[dbName]
				columnSlice = append(columnSlice, "? ?")
				args = append(args,
					clause.Column{Name: dbName},
					m.DB.Migrator().FullDataTypeOf(field),
				)
			}
			columnStr := strings.Join(columnSlice, ",")

			// Step 2. Build constraint check SQL string if any constraint
			constrSlice := make([]string, 0, len(columnSlice))
			for _, check := range stmt.Schema.ParseCheckConstraints() {
				constrSlice = append(constrSlice, "CONSTRAINT ? CHECK ?")
				args = append(args,
					clause.Column{Name: check.Name},
					clause.Expr{SQL: check.Constraint},
				)
			}
			constrStr := strings.Join(constrSlice, ",")
			if len(constrSlice) > 0 {
				constrStr = ", " + constrStr
			}

			// Step 3. Build index SQL string
			// NOTE: index class [UNIQUE | FULLTEXT | SPATIAL] is NOT supported!
			indexSlice := make([]string, 0, 10)
			for _, index := range stmt.Schema.ParseIndexes() {
				if m.CreateIndexAfterCreateTable {
					defer func(model interface{}, indexName string) {
						// TODO (iqdf): what if there are multiple errors
						// when creating indices after create table?
						err = tx.Migrator().CreateIndex(model, indexName)
					}(model, index.Name)
					continue
				}
				// TODO(iqdf): support primary key by put it as pass the fieldname
				// as MergeTree(...) parameters. But somehow it complained.
				// Note that primary key doesn't ensure uniqueness

				// Get indexing type `gorm:"index,type:minmax"`
				// Choice: minmax | set(n) | ngrambf_v1(n, size, hash, seed) | bloomfilter()
				indexType := DefaultIndexType
				if index.Type != "" {
					indexType = index.Type
				}

				// Get expression for index options
				// Syntax: (`colname1`, ...)
				buildIndexOptions := tx.Migrator().(migrator.BuildIndexOptionsInterface)
				indexOptions := buildIndexOptions.BuildIndexOptions(index.Fields, stmt)

				// Stringify index builder
				// TODO (iqdf): support granularity
				str := fmt.Sprintf("INDEX ? ? TYPE %s GRANULARITY %d", indexType, DefaultGranularity)
				indexSlice = append(indexSlice, str)
				args = append(args, clause.Expr{SQL: index.Name}, indexOptions)
			}
			indexStr := strings.Join(indexSlice, ", ")
			if len(indexSlice) > 0 {
				indexStr = ", " + indexStr
			}

			// Step 4. Finally assemble CREATE TABLE ... SQL string
			engineOpts := "MergeTree() ORDER BY tuple()"
			createTableSQL = fmt.Sprintf(createTableSQL, columnStr, constrStr, indexStr, engineOpts)

			fmt.Println("Exec Create Table:", createTableSQL)
			err = tx.Exec(createTableSQL, args...).Error

			return
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m Migrator) HasTable(value interface{}) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		currentDatabase := m.DB.Migrator().CurrentDatabase()
		return m.DB.Raw(
			"SELECT count(*) FROM system.tables WHERE database = ? AND name = ? AND is_temporary = ?",
			currentDatabase,
			stmt.Table,
			uint8(0)).Row().Scan(&count)
	})
	return count > 0
}

// Columns

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
		return fmt.Errorf("altercolumn() failed to look up column with name: %s", field)
	})
}

// NOTE: Only supported after ClickHouse 20.4 and above.
// See: https://github.com/ClickHouse/ClickHouse/issues/146
func (m Migrator) RenameColumn(value interface{}, oldName, newName string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if !m.Dialector.DontSupportRenameColumn {
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
			return fmt.Errorf("renamecolumn() failed to look up column with name: %s", oldName)
		}
		return ErrRenameIndexUnsupported
	})
}

func (m Migrator) HasColumn(value interface{}, field string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		currentDatabase := m.DB.Migrator().CurrentDatabase()
		name := field
		if field := stmt.Schema.LookUpField(field); field != nil {
			name = field.DBName
		}

		return m.DB.Raw(
			"SELECT count(*) FROM system.columns WHERE database = ? AND table = ? AND name = ?",
			currentDatabase, stmt.Table, name,
		).Row().Scan(&count)
	})

	return count > 0
}

// Indexes

func (m Migrator) BuildIndexOptions(opts []schema.IndexOption, stmt *gorm.Statement) (results []interface{}) {
	for _, indexOpt := range opts {
		str := stmt.Quote(indexOpt.DBName)
		if indexOpt.Expression != "" {
			str = indexOpt.Expression
		}
		results = append(results, clause.Expr{SQL: str})
	}
	return
}

func (m Migrator) CreateIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			opts := m.BuildIndexOptions(idx.Fields, stmt)
			values := []interface{}{
				clause.Table{Name: stmt.Table},
				clause.Column{Name: idx.Name},
				opts,
			}

			// Get indexing type `gorm:"index,type:minmax"`
			// Choice: minmax | set(n) | ngrambf_v1(n, size, hash, seed) | bloomfilter()
			indexType := DefaultIndexType
			if idx.Type != "" {
				indexType = idx.Type
			}

			// NOTE: concept of UNIQUE | FULLTEXT | SPATIAL index
			// is NOT supported in clickhouse
			createIndexSQL := "ALTER TABLE ? ADD INDEX ? ? TYPE %s GRANULARITY %d"      // TODO(iqdf): how to inject Granularity
			createIndexSQL = fmt.Sprintf(createIndexSQL, indexType, DefaultGranularity) // Granularity: 1 (default)
			return m.DB.Exec(createIndexSQL, values...).Error
		}
		return ErrCreateIndexFailed
	})
}

func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	// TODO(iqdf): drop index and add the index again with different name
	// DROP INDEX ?
	// ADD INDEX ? TYPE ? GRANULARITY ?
	return ErrRenameIndexUnsupported
}

func (m Migrator) DropIndex(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}
		dropIndexSQL := "ALTER TABLE ? DROP INDEX ?"
		return m.DB.Exec(dropIndexSQL,
			clause.Table{Name: stmt.Table},
			clause.Column{Name: name}).Error
	})
}
