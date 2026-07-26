package main

import (
	"cmp"
	"log"
	"os"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/xo/dburl"
)

type User struct {
	ID    int64  `db:"id" json:"id"`
	Name  string `db:"name" json:"name"`
	Email string `db:"email" json:"email"`
}

func main() {
	rawDB, err := dburl.Open(os.Getenv("DATABASE_URL"))

	if err != nil {
		log.Fatalf("open database: %v", err)
	}

	db := sqlx.NewDb(rawDB, "mysql")
	defer db.Close()

	router := gin.Default()

	router.GET("/", func(c *gin.Context) {
		users := make([]User, 0)

		if err := db.SelectContext(c.Request.Context(), &users, "SELECT id, name, email FROM users"); err != nil {
			c.JSON(500, gin.H{"error": "failed to fetch users"})
			return
		}

		c.JSON(200, users)
	})

	if err := router.Run(":" + cmp.Or(os.Getenv("PORT"), "3000")); err != nil {
		log.Fatal(err)
	}
}
