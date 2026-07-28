package main

import (
	"cmp"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
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

	cacheOptions, err := redis.ParseURL(os.Getenv("REDIS_URL"))
	if err != nil {
		log.Fatalf("parse REDIS_URL: %v", err)
	}
	cache := redis.NewClient(cacheOptions)
	defer cache.Close()

	router := gin.Default()

	router.GET("/", func(c *gin.Context) {
		users := make([]User, 0)

		if err := db.SelectContext(c.Request.Context(), &users, "SELECT id, name, email FROM users"); err != nil {
			c.JSON(500, gin.H{"error": "failed to fetch users"})
			return
		}

		c.JSON(200, users)
	})

	router.GET("/valkey", func(c *gin.Context) {
		ctx := c.Request.Context()
		pubsub := cache.Subscribe(ctx, "events")
		defer pubsub.Close()

		if _, err := pubsub.Receive(ctx); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to subscribe"})
			return
		}

		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")
		c.Header("X-Accel-Buffering", "no")
		c.Status(http.StatusOK)
		c.Writer.Flush()

		messages := pubsub.Channel()
		heartbeat := time.NewTicker(15 * time.Second)
		defer heartbeat.Stop()

		for {
			select {
			case message, ok := <-messages:
				if !ok {
					return
				}
				c.SSEvent("message", message.Payload)
				c.Writer.Flush()
			case <-heartbeat.C:
				fmt.Fprint(c.Writer, ": keep-alive\n\n")
				c.Writer.Flush()
			case <-ctx.Done():
				return
			}
		}
	})

	if err := router.Run(":" + cmp.Or(os.Getenv("PORT"), "3000")); err != nil {
		log.Fatal(err)
	}
}
