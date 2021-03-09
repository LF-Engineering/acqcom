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

func acqcom(db *sqlx.DB, when time.Time, oid, nid int) {
	id, uuid, start, end, pSlug, role, cnt, i, n := 0, "", time.Now(), time.Now(), "", "", 0, 0, 0
	rows, err := db.Query("select count(id) from enrollments where organization_id = ? and end > ?", oid, when)
	for rows.Next() {
		err = rows.Scan(&n)
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
	fmt.Printf("%d enrollments to process\n", n)
	tx, err := db.Begin()
	if err != nil {
		fmt.Printf("error %+v\n", err)
		return
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	rows, err = db.Query("select id, uuid, start, end, project_slug, role from enrollments where organization_id = ? and end > ?", oid, when)
	for rows.Next() {
		err = rows.Scan(&id, &uuid, &start, &end, &pSlug, &role)
		if err != nil {
			return
		}
		i++
		if i%25 == 0 {
			fmt.Printf("%d/%d\n", i, n)
		}
		rows2, err := tx.Query("select count(id) from enrollments where organization_id = ? and uuid = ? and project_slug = ? and (start < ? or end > ?)", nid, uuid, pSlug, end, start)
		//rows2, err := tx.Query("select count(id) from enrollments where organization_id = ? and uuid = ? and project_slug = ?", nid, uuid, pSlug)
		for rows2.Next() {
			err = rows2.Scan(&cnt)
			if err != nil {
				return
			}
		}
		err = rows2.Err()
		if err != nil {
			return
		}
		err = rows2.Close()
		if err != nil {
			return
		}
		if !start.Before(when) {
			if cnt > 0 {
				fmt.Printf("enrollment %d needs manual update, its start & end dates are after %v but also has new company's enrollments\n", id, when)
				continue
			}
			_, err = tx.Exec("update enrollments set organization_id = ? where id = ?", nid, id)
			if err != nil {
				fmt.Printf("error %+v\n", err)
				return
			}
			continue
		}
		if cnt > 0 {
			fmt.Printf("enrollment %d needs manual update, it intersects %v but also has new company's enrollments 'select * from enrollments where uuid = '%s' and organization_id in (%d, %d) order by start, end'\n", id, when, uuid, oid, nid)
			continue
		}
		_, err = tx.Exec("update enrollments set end = ? where id = ?", when, id)
		if err != nil {
			fmt.Printf("error %+v\n", err)
			return
		}
		_, err = tx.Exec("insert into enrollments(uuid, organization_id, start, end, project_slug, role) values(?, ?, ?, ?, ?, ?)", uuid, nid, when, end, pSlug, role)
		if err != nil {
			fmt.Printf("error %+v\n", err)
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
	err = tx.Commit()
	if err != nil {
		fmt.Printf("error %+v\n", err)
		return
	}
	tx = nil
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
	when, err := time.Parse("2006-01-02", os.Getenv("DT"))
	if err != nil {
		fmt.Printf("error %+v\n", err)
		return
	}
	acqcom(db, when, oid, nid)
}
