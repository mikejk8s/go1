package insert

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
	i := item{
		name:  "iphone",
		price: 950,
	}
	err = insert(db, i)
	if err != nil {
		log.Printf("Insert item failed with error %s", err)
		return
	}

	i1 := item{
		name:  "Axe",
		price: 990,
	}
	i2 := item{
		name:  "Sword",
		price: 500,
	}
	err = multipleInsert(db, []item{i1, i2})
	if err != nil {
		log.Printf("Multiple insert failed with error %s", err)
		return
	}
}
