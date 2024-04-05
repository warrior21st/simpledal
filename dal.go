package simpledal

import (
	"database/sql"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/warrior21st/go-utils/commonutil"
)

func ExecuteScalar(db *sqlx.DB, sqlStatements string, args ...interface{}) string {
	rows, err := db.Queryx(addLimit1(sqlStatements), args...)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return ""
	}

	var d []byte
	err = rows.Scan(&d)
	if err != nil {
		panic(err)
	}

	return string(d)
}

func ExecuteScalarInt(db *sqlx.DB, sqlStatements string, args ...interface{}) int64 {
	s := ExecuteScalar(db, sqlStatements, args...)
	if s == "" {
		return 0
	}

	return commonutil.ParseInt64(s)
}

func ExecuteScalarIntWithTx(tx *sqlx.Tx, sqlStatements string, args ...interface{}) int64 {
	s := ExecuteScalarWithTx(tx, sqlStatements, args...)
	if s == "" {
		return 0
	}

	return commonutil.ParseInt64(s)
}

func ExecuteScalarFloat(db *sqlx.DB, sqlStatements string, args ...interface{}) float64 {
	s := ExecuteScalar(db, sqlStatements, args...)
	if s == "" {
		return 0
	}

	return commonutil.ParseFloat64(s)
}

func ExecuteScalarFloatWithTx(tx *sqlx.Tx, sqlStatements string, args ...interface{}) float64 {
	s := ExecuteScalarWithTx(tx, sqlStatements, args...)
	if s == "" {
		return 0
	}

	return commonutil.ParseFloat64(s)
}

func ExecuteScalarWithTx(tx *sqlx.Tx, sqlStatements string, args ...interface{}) string {
	rows, err := tx.Queryx(addLimit1(sqlStatements), args...)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return ""
	}

	var d []byte
	err = rows.Scan(&d)
	if err != nil {
		panic(err)
	}

	return string(d)
}

func QueryRows(db *sqlx.DB, sqlStatements string, args ...interface{}) *sqlx.Rows {
	rows, err := db.Queryx(sqlStatements, args...)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	return rows
}

func QueryFirstToMap(db *sqlx.DB, sqlStatements string, args ...interface{}) map[string]string {
	datas := QueryToMap(db, addLimit1(sqlStatements), args...)
	if len(datas) == 0 {
		return nil
	}

	return datas[0]
}

func QueryToMap(db *sqlx.DB, sqlStatements string, args ...interface{}) []map[string]string {
	rows, err := db.Queryx(sqlStatements, args...)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	return scanRowsToMaps(rows)
}

func QueryFirstToMapWithTx(tx *sqlx.Tx, sqlStatements string, args ...interface{}) map[string]string {
	datas := QueryToMapWithTx(tx, addLimit1(sqlStatements), args...)
	if len(datas) == 0 {
		return nil
	}

	return datas[0]
}

func QueryToMapWithTx(tx *sqlx.Tx, sqlStatements string, args ...interface{}) []map[string]string {
	rows, err := tx.Queryx(sqlStatements, args...)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	return scanRowsToMaps(rows)
}

func scanRowsToMaps(rows *sqlx.Rows) []map[string]string {

	cols, err := rows.Columns()
	if err != nil {
		panic(err)
	}

	vals := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range vals {
		scans[i] = &vals[i]
	}

	results := make([]map[string]string, 0)
	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			panic(err)
		}

		m := make(map[string]string)
		for i, v := range vals {
			if v == nil {
				m[cols[i]] = ""
			} else {
				m[cols[i]] = string(v)
			}
		}

		results = append(results, m)
	}

	return results
}

func addLimit1(sql string) string {
	lastIndex := strings.LastIndex(sql, " limit ")
	if lastIndex != -1 {
		return sql
	}

	return strings.Join([]string{sql, " limit 1"}, "")
}

func MustExec(db *sqlx.DB, sqlStatements string, args ...interface{}) sql.Result {
	return db.MustExec(sqlStatements, args...)
}

func MustExecWithTx(tx *sqlx.Tx, sqlStatements string, args ...interface{}) sql.Result {
	res, err := tx.Exec(sqlStatements, args...)
	if err != nil {
		MustRollback(tx)
		panic(err)
	}

	return res
}

func MustRollback(tx *sqlx.Tx) {
	err := tx.Rollback()
	if err != nil {
		panic(err)
	}
}

func MustCommit(tx *sqlx.Tx) {
	err := tx.Commit()
	if err != nil {
		// tx.Rollback()
		panic(err)
	}
}

func InsertByNamedValues(db *sqlx.DB, table string, colVals map[string]interface{}) (result sql.Result, err error) {
	result, err = db.NamedExec(GenInsertSqlByNamed(table, colVals), colVals)
	return
}

func InsertByNamedValuesWithTx(tx *sqlx.Tx, table string, colVals map[string]interface{}) (result sql.Result, err error) {
	result, err = tx.NamedExec(GenInsertSqlByNamed(table, colVals), colVals)
	return
}

func MustInsertByNamedValues(db *sqlx.DB, table string, colVals map[string]interface{}) sql.Result {
	res, err := db.NamedExec(GenInsertSqlByNamed(table, colVals), colVals)
	if err != nil {
		panic(err)
	}

	return res
}

func MustInsertByNamedValuesWithTx(tx *sqlx.Tx, table string, colVals map[string]interface{}) sql.Result {
	res, err := tx.NamedExec(GenInsertSqlByNamed(table, colVals), colVals)
	if err != nil {
		MustRollback(tx)
		panic(err)
	}

	return res
}

func GenInsertSqlByNamed(table string, colVals map[string]interface{}) string {
	var colSqlBuf strings.Builder
	colSqlBuf.WriteString("insert into ")
	colSqlBuf.WriteString(table)
	colSqlBuf.WriteString(" (")

	var valSqlbuf strings.Builder
	valSqlbuf.WriteString(" values (")

	for k := range colVals {
		colSqlBuf.WriteString(k)
		colSqlBuf.WriteString(",")

		valSqlbuf.WriteString(":")
		valSqlbuf.WriteString(k)
		valSqlbuf.WriteString(",")
	}

	sql := strings.Join([]string{colSqlBuf.String()[:colSqlBuf.Len()-1], ") ", valSqlbuf.String()[:valSqlbuf.Len()-1], ")"}, "")

	return sql
}
