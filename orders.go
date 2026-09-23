package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
)

const TypeProcessOrder = "order:process"

type Order struct {
	ID         int64   `db:"id" json:"id"`
	Status     string  `db:"status" json:"status"`
	Worker     *string `db:"worker" json:"worker"`
	CreatedAt  string  `db:"created_at" json:"created_at"`
	DurationMs *int64  `db:"duration_ms" json:"duration_ms"`
}

type processOrderPayload struct {
	OrderID int64 `json:"order_id"`
}

func migrate(db *sqlx.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS orders (
			id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
			status VARCHAR(20) NOT NULL DEFAULT 'pending',
			worker VARCHAR(255) NULL,
			created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			completed_at DATETIME(3) NULL
		)
	`)

	return err
}

// dispatchOrder inserts a pending order and pushes a job onto the queue to process it.
func dispatchOrder(ctx context.Context, db *sqlx.DB, queue *asynq.Client) (int64, error) {
	result, err := db.ExecContext(ctx, "INSERT INTO orders (status) VALUES ('pending')")

	if err != nil {
		return 0, fmt.Errorf("insert order: %w", err)
	}

	id, err := result.LastInsertId()

	if err != nil {
		return 0, fmt.Errorf("insert order: %w", err)
	}

	payload, _ := json.Marshal(processOrderPayload{OrderID: id})

	if _, err := queue.EnqueueContext(ctx, asynq.NewTask(TypeProcessOrder, payload), asynq.MaxRetry(3)); err != nil {
		db.ExecContext(ctx, "UPDATE orders SET status = 'failed' WHERE id = ?", id)
		return id, fmt.Errorf("enqueue order %d: %w", id, err)
	}

	return id, nil
}

// handleProcessOrder is the job the worker runs: it marks the order as processing,
// simulates some work, then marks it complete.
func handleProcessOrder(db *sqlx.DB) asynq.HandlerFunc {
	host, _ := os.Hostname()

	return func(ctx context.Context, t *asynq.Task) error {
		var p processOrderPayload

		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return fmt.Errorf("decode payload: %v: %w", err, asynq.SkipRetry)
		}

		if _, err := db.ExecContext(ctx, "UPDATE orders SET status = 'processing', worker = ? WHERE id = ?", host, p.OrderID); err != nil {
			return err
		}

		select {
		case <-time.After(time.Second + rand.N(2*time.Second)):
		case <-ctx.Done():
			return ctx.Err()
		}

		if _, err := db.ExecContext(ctx, "UPDATE orders SET status = 'complete', completed_at = NOW(3) WHERE id = ?", p.OrderID); err != nil {
			return err
		}

		log.Printf("processed order %d", p.OrderID)

		return nil
	}
}

func recentOrders(ctx context.Context, db *sqlx.DB) ([]Order, map[string]int, error) {
	orders := make([]Order, 0)

	err := db.SelectContext(ctx, &orders, `
		SELECT
			id,
			status,
			worker,
			DATE_FORMAT(created_at, '%H:%i:%s') AS created_at,
			TIMESTAMPDIFF(MICROSECOND, created_at, completed_at) DIV 1000 AS duration_ms
		FROM orders
		ORDER BY id DESC
		LIMIT 100
	`)

	if err != nil {
		return nil, nil, err
	}

	rows, err := db.QueryxContext(ctx, "SELECT status, COUNT(*) FROM orders GROUP BY status")

	if err != nil {
		return nil, nil, err
	}

	defer rows.Close()

	counts := map[string]int{"pending": 0, "processing": 0, "complete": 0}

	for rows.Next() {
		var status string
		var count int

		if err := rows.Scan(&status, &count); err != nil {
			return nil, nil, err
		}

		counts[status] = count
	}

	return orders, counts, rows.Err()
}
