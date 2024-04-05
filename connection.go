package simpledal

import (
	"log"
	"time"

	"github.com/jmoiron/sqlx"
)

func GetMysqlNewConn(dsn string) *sqlx.DB {
	return GetNewConn("mysql", dsn)
}

func GetPostgreNewConn(dsn string) *sqlx.DB {
	return GetNewConn("postgres", dsn)
}

// dirverName: mysql/postgres
func GetNewConn(dirverName string, dsn string) *sqlx.DB {
	db, err := sqlx.Connect(dirverName, dsn)
	if err != nil {
		log.Panic(err)
	}
	db.SetConnMaxLifetime(time.Duration(8) * time.Second)

	return db
}
