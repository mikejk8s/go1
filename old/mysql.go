package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	username = "root"
	password = "password"
	hostname = "127.0.0.1:3306"
	dbname   = "ecommerce"
)

type item struct {
	name  string
	price int
}

func dsn(dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, hostname, dbName)
}

func dbConnection() (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn(""))
	if err != nil {
		log.Printf("Error %s when opening DB\n", err)
		return nil, err
	}
	//defer db.Close()

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	res, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+dbname)
	if err != nil {
		log.Printf("Error %s when creating DB\n", err)
		return nil, err
	}
	no, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when fetching rows", err)
		return nil, err
	}
	log.Printf("rows affected %d\n", no)

	db.Close()
	db, err = sql.Open("mysql", dsn(dbname))
	if err != nil {
		log.Printf("Error %s when opening DB", err)
		return nil, err
	}
	//defer db.Close()

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(time.Minute * 5)

	ctx, cancelfunc = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	err = db.PingContext(ctx)
	if err != nil {
		log.Printf("Errors %s pinging DB", err)
		return nil, err
	}
	log.Printf("Connected to DB %s successfully\n", dbname)
	return db, nil
}

func createItemTable(db *sql.DB) error {
	query := `CREATE TABLE IF NOT EXISTS item(item_id int primary key auto_increment, item_name text, 
        item_price int, created_at datetime default CURRENT_TIMESTAMP, updated_at datetime default CURRENT_TIMESTAMP)`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	res, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when creating item table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when getting rows affected", err)
		return err
	}
	log.Printf("Rows affected when creating table: %d", rows)
	return nil
}

func insert(db *sql.DB, p item) error {
	query := "INSERT INTO item(item_name, item_price) VALUES (?, ?)"
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return err
	}
	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, p.name, p.price)
	if err != nil {
		log.Printf("Error %s when inserting row into items table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when finding rows affected", err)
		return err
	}
	log.Printf("%d items created ", rows)
	prdID, err := res.LastInsertId()
	if err != nil {
		log.Printf("Error %s when getting last inserted item", err)
		return err
	}
	log.Printf("Item with ID %d created", prdID)
	return nil
}

func multipleInsert(db *sql.DB, items []item) error {
	query := "INSERT INTO item(item_name, item_price) VALUES "
	var inserts []string
	var params []interface{}
	for _, v := range items {
		inserts = append(inserts, "(?, ?)")
		params = append(params, v.name, v.price)
	}
	queryVals := strings.Join(inserts, ",")
	query = query + queryVals
	log.Println("query is", query)
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return err
	}
	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, params...)
	if err != nil {
		log.Printf("Error %s when inserting row into items table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when finding rows affected", err)
		return err
	}
	log.Printf("%d items created simultaneously", rows)
	return nil
}

func selectPrice(db *sql.DB, itemName string) (int, error) {
	log.Printf("Getting item price")
	query := `select item_price from item where item_name = ?`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return 0, err
	}
	defer stmt.Close()
	var price int
	row := stmt.QueryRowContext(ctx, itemName)
	if err := row.Scan(&price); err != nil {
		return 0, err
	}
	return price, nil
}

func selectItemsByPrice(db *sql.DB, minPrice int, maxPrice int) ([]item, error) {
	log.Printf("Getting items by price")
	query := `select item_name, item_price from item where item_price >= ? && item_price <= ?;`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return []item{}, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx, minPrice, maxPrice)
	if err != nil {
		return []item{}, err
	}
	defer rows.Close()
	var items = []item{}
	for rows.Next() {
		var prd item
		if err := rows.Scan(&prd.name, &prd.price); err != nil {
			return []item{}, err
		}
		items = append(items, prd)
	}
	if err := rows.Err(); err != nil {
		return []item{}, err
	}
	return items, nil
}

func main() {
	db, err := dbConnection()
	if err != nil {
		log.Printf("Error %s when getting db connection", err)
		return
	}
	defer db.Close()
	log.Printf("Successfully connected to database")
	err = createItemTable(db)
	if err != nil {
		log.Printf("Create item table failed with error %s", err)
		return
	}
	p := item{
		name:  "iphone",
		price: 950,
	}
	err = insert(db, p)
	if err != nil {
		log.Printf("Insert item failed with error %s", err)
		return
	}

	p1 := item{
		name:  "Galaxy",
		price: 990,
	}
	p2 := item{
		name:  "iPad",
		price: 500,
	}
	err = multipleInsert(db, []item{p1, p2})
	if err != nil {
		log.Printf("Multiple insert failed with error %s", err)
		return
	}

	itemName := "iphone"
	price, err := selectPrice(db, itemName)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("Item %s not found in DB", itemName)
	case err != nil:
		log.Printf("Encountered err %s when fetching price from DB", err)
	default:
		log.Printf("Price of %s is %d", itemName, price)
	}

	minPrice := 900
	maxPrice := 1000
	items, err := selectItemsByPrice(db, minPrice, maxPrice)
	if err != nil {
		log.Printf("Error %s when selecting item by price", err)
		return
	}
	for _, item := range items {
		log.Printf("Name: %s Price: %d", item.name, item.price)
	}
}
