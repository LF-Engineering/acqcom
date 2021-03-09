package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func initDB() *sqlx.DB {
	dbURL := os.Getenv("DB")
	if !strings.Contains(dbURL, "parseTime=true") {
		if strings.Contains(dbURL, "?") {
			dbURL += "&parseTime=true"
		} else {
			dbURL += "?parseTime=true"
		}
	}
	d, err := sqlx.Connect("mysql", dbURL)
	if err != nil {
		fmt.Printf("error: %+v\n", err)
		panic("unable to connect to affiliation database: " + dbURL)
	}
	d.SetConnMaxLifetime(30 * time.Second)
	return d
}

func acqcom(db *sqlx.DB, when string, oid, nid int) {
	id, uuid, start, end, pSlug, role := 0, "", time.Now(), time.Now(), "", ""
	rows, err := db.Query("select id, uuid, start, end, project_slug, role from enrollments where organization_id = ? and end > ?", oid, when)
	for rows.Next() {
		err = rows.Scan(&id, &uuid, &start, &end, &pSlug, &role)
		if err != nil {
			return
		}
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
}

func main() {
	db := initDB()
	oid, nid := 0, 0
	rows, err := db.Query("select id from organizations where name = ?", os.Getenv("OLD_NAME"))
	for rows.Next() {
		err = rows.Scan(&oid)
		if err != nil {
			return
		}
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	rows, err = db.Query("select id from organizations where name = ?", os.Getenv("NEW_NAME"))
	for rows.Next() {
		err = rows.Scan(&nid)
		if err != nil {
			return
		}
	}
	err = rows.Err()
	if err != nil {
		return
	}
	err = rows.Close()
	if err != nil {
		return
	}
	if oid == 0 || nid == 0 {
		return
	}
	when := os.Getenv("DT")
	acqcom(db, when, oid, nid)
}
