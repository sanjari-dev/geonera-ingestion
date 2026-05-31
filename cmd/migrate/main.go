package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	if err := godotenv.Load(".env"); err != nil {
		log.Printf("warning: .env not loaded (%v), relying on OS environment", err)
	}

	dsn := os.Getenv("DATABASE_DIRECT_URL")
	if dsn == "" {
		log.Fatal("DATABASE_DIRECT_URL is not set")
	}

	sqlBytes, err := os.ReadFile("database/init.sql")
	if err != nil {
		log.Fatalf("read init.sql: %v", err)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("db close: %v", err)
		}
	}()

	if err := db.PingContext(context.Background()); err != nil {
		log.Fatalf("ping: %v", err)
	}

	if _, err := db.ExecContext(context.Background(), string(sqlBytes)); err != nil {
		log.Fatalf("exec init.sql: %v", err)
	}

	fmt.Println("init.sql executed successfully — schemas, tables, and seed data are ready.")
}
