package db
import (
	"github.com/jackc/pgx/v5/pgxpool"
	"context"
)

func NewPostgres(ctx context.Context, connString string) (*pgxpool.Pool,error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
        return nil, err
    }
	return pool,nil 
}
