package database

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Open(ctx context.Context, connectionURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, connectionURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}
