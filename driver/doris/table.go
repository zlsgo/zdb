//go:build doris
// +build doris

package doris

import (
	"bytes"
	"fmt"
	"strings"
)

func (c *Config) GetTableOptions() string {
	var options bytes.Buffer

	engine := "OLAP"
	if c.Engine != "" {
		engine = c.Engine
	}
	options.WriteString(fmt.Sprintf(" ENGINE=%s", engine))

	if len(c.DuplicateKeys) > 0 {
		options.WriteString("\nDUPLICATE KEY(")
		options.WriteString(strings.Join(c.DuplicateKeys, ", "))
		options.WriteString(")")
	}

	distributedBy := c.DistributedBy
	if distributedBy == "" && len(c.DuplicateKeys) > 0 {
		distributedBy = fmt.Sprintf("HASH(%s)", c.DuplicateKeys[0])
	}

	if distributedBy != "" {
		options.WriteString("\nDISTRIBUTED BY ")
		options.WriteString(distributedBy)
	}

	buckets := c.Buckets
	if buckets <= 0 {
		buckets = 10 // 默认10个分桶
	}
	options.WriteString(fmt.Sprintf(" BUCKETS %d", buckets))

	options.WriteString("\nPROPERTIES (")

	replicationNum := c.ReplicationNum
	if replicationNum <= 0 {
		replicationNum = 1 // 默认1个副本
	}
	options.WriteString(fmt.Sprintf("\n  \"replication_num\" = \"%d\"", replicationNum))

	options.WriteString(fmt.Sprintf(",\n  \"in_memory\" = \"%t\"", c.InMemory))

	storageFormat := c.StorageFormat
	if storageFormat == "" {
		storageFormat = "V2" // 默认V2存储格式
	}
	options.WriteString(fmt.Sprintf(",\n  \"storage_format\" = \"%s\"", storageFormat))

	if c.CustomProperties != nil {
		for k, v := range c.CustomProperties {
			options.WriteString(fmt.Sprintf(",\n  \"%s\" = \"%s\"", k, v))
		}
	}

	options.WriteString("\n)")

	return options.String()
}

func (c *Config) ModifyCreateTableSQL(sql string) string {
	if strings.Contains(strings.ToUpper(sql), "ENGINE=") {
		return sql
	}

	sql = replaceQuotesInCreateTable(sql)

	endPos := strings.LastIndex(sql, ")")
	if endPos == -1 {
		return sql
	}

	return sql[:endPos+1] + c.GetTableOptions()
}

func replaceQuotesInCreateTable(sql string) string {
	lowerSQL := strings.ToLower(sql)

	if !strings.Contains(lowerSQL, "create table") {
		return sql
	}

	parts := strings.SplitN(sql, "(", 2)
	if len(parts) != 2 {
		return sql
	}

	tableNamePart := parts[0]
	tableNamePart = strings.Replace(tableNamePart, "\"", "`", -1)

	columns := convertColumnsToDorisFormat(parts[1])

	if strings.HasSuffix(columns, ")") {
		columns = columns[:len(columns)-1]
	}

	return tableNamePart + "(" + columns + ")"
}

func convertColumnsToDorisFormat(columnsStr string) string {
	columns := strings.Split(columnsStr, ",")
	result := make([]string, 0, len(columns))

	for _, column := range columns {

		column = strings.TrimSpace(column)
		if column == ")" || column == "" {
			continue
		}

		// 将双引号替换为反引号
		if idx := strings.Index(column, "\""); idx >= 0 {
			endIdx := strings.Index(column[idx+1:], "\"")
			if endIdx >= 0 {
				endIdx += idx + 1 // 调整为全局索引
				columnName := column[idx+1 : endIdx]
				column = column[:idx] + "`" + columnName + "`" + column[endIdx+1:]
			}
		}

		column = convertColumnTypeAndConstraints(column)

		result = append(result, column)
	}

	return strings.Join(result, ",\n  ")
}

func convertColumnTypeAndConstraints(column string) string {
	parts := strings.SplitN(column, " ", 2)
	if len(parts) < 2 {
		return column // 如果没有空格，直接返回
	}

	columnName := parts[0]
	columnDef := parts[1]

	isPrimaryKey := strings.Contains(strings.ToUpper(columnDef), "PRIMARY KEY")

	// 处理数据类型
	columnDef = strings.Replace(columnDef, "PRIMARY KEY", "", -1)
	columnDef = strings.TrimSpace(columnDef)

	isDistributionKey := isPrimaryKey // 主键通常也是分布式键

	columnDef = convertDataType(columnDef, isDistributionKey)

	if isPrimaryKey && !strings.Contains(strings.ToUpper(columnDef), "NOT NULL") {
		columnDef += " NOT NULL"
	}

	if isPrimaryKey && !strings.Contains(columnDef, "COMMENT") {
		columnDef += " COMMENT 'Primary Key'"
	}

	return columnName + " " + columnDef
}

func convertDataType(dataType string, forceBigInt ...bool) string {
	lowerType := strings.ToLower(dataType)
	useBigInt := len(forceBigInt) > 0 && forceBigInt[0]

	switch {
	case strings.HasPrefix(lowerType, "integer") || strings.HasPrefix(lowerType, "int"):
		if useBigInt {
			return "BIGINT" + dataType[strings.Index(lowerType, "int")+3:]
		}

		if strings.HasPrefix(lowerType, "integer") {
			return "BIGINT" + dataType[len("integer"):]
		}
		return "INT" + dataType[len("int"):]
	case strings.HasPrefix(lowerType, "text"):
		return "STRING" + dataType[len("text"):]
	case strings.HasPrefix(lowerType, "blob"):
		return "STRING" + dataType[len("blob"):]
	case strings.HasPrefix(lowerType, "varchar"):
		return "VARCHAR" + dataType[len("varchar"):]
	case strings.HasPrefix(lowerType, "char"):
		return "CHAR" + dataType[len("char"):]
	case strings.HasPrefix(lowerType, "float"):
		return "FLOAT" + dataType[len("float"):]
	case strings.HasPrefix(lowerType, "double"):
		return "DOUBLE" + dataType[len("double"):]
	case strings.HasPrefix(lowerType, "boolean"):
		return "BOOLEAN" + dataType[len("boolean"):]
	case strings.HasPrefix(lowerType, "datetime"):
		return "DATETIME" + dataType[len("datetime"):]
	case strings.HasPrefix(lowerType, "date"):
		return "DATE" + dataType[len("date"):]
	default:
		return dataType
	}
}
