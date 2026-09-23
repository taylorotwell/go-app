package main

import (
	"cmp"
	_ "embed"
	"log"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/xo/dburl"
)

//go:embed dashboard.html
var dashboardHTML []byte

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

	redis, err := asynq.ParseRedisURI(os.Getenv("REDIS_URL"))

	if err != nil {
		log.Fatalf("parse REDIS_URL: %v", err)
	}

	if len(os.Args) > 1 && os.Args[1] == "worker" {
		runWorker(db, redis)
		return
	}

	runWeb(db, redis)
}

func runWeb(db *sqlx.DB, redis asynq.RedisConnOpt) {
	if err := migrate(db); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	queue := asynq.NewClient(redis)
	defer queue.Close()

	router := gin.Default()

	router.GET("/", func(c *gin.Context) {
		users := make([]User, 0)

		if err := db.SelectContext(c.Request.Context(), &users, "SELECT id, name, email FROM users"); err != nil {
			c.JSON(500, gin.H{"error": "failed to fetch users"})
			return
		}

		c.JSON(200, users)
	})

	router.GET("/dashboard", func(c *gin.Context) {
		c.Data(200, "text/html; charset=utf-8", dashboardHTML)
	})

	router.GET("/orders", func(c *gin.Context) {
		orders, counts, err := recentOrders(c.Request.Context(), db)

		if err != nil {
			c.JSON(500, gin.H{"error": "failed to fetch orders"})
			return
		}

		c.JSON(200, gin.H{"orders": orders, "counts": counts})
	})

	router.POST("/orders", func(c *gin.Context) {
		count, _ := strconv.Atoi(c.DefaultQuery("count", "1"))
		count = min(max(count, 1), 100)

		ids := make([]int64, 0, count)

		for range count {
			id, err := dispatchOrder(c.Request.Context(), db, queue)

			if err != nil {
				log.Printf("dispatch order: %v", err)
				c.JSON(500, gin.H{"error": "failed to dispatch order", "dispatched": ids})
				return
			}

			ids = append(ids, id)
		}

		c.JSON(201, gin.H{"dispatched": ids})
	})

	if err := router.Run(":" + cmp.Or(os.Getenv("PORT"), "3000")); err != nil {
		log.Fatal(err)
	}
}

func runWorker(db *sqlx.DB, redis asynq.RedisConnOpt) {
	srv := asynq.NewServer(redis, asynq.Config{Concurrency: 10})

	mux := asynq.NewServeMux()
	mux.HandleFunc(TypeProcessOrder, handleProcessOrder(db))

	// Run blocks until SIGTERM/SIGINT, then waits for in-flight jobs to finish.
	if err := srv.Run(mux); err != nil {
		log.Fatal(err)
	}
}
