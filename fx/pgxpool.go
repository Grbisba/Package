package fx

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxUUID "github.com/vgarvardt/pgx-google-uuid/v5"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"time"
)

const (
	RetryAttempts = 5
	RetryDelay    = 3 * time.Second
)

// New opens new postgres connection, configures it and return prepared pool.
func New(lc fx.Lifecycle, dbUri string, log *zap.Logger) (*pgxpool.Pool, error) {
	var pool *pgxpool.Pool

	configuredPool, err := pgxpool.ParseConfig(dbUri)
	if err != nil {
		return nil, fmt.Errorf("error while parsing db uri: %w", err)
	}

	configuredPool.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		pgxUUID.Register(conn.TypeMap())
		return nil
	}

	pool, err = pgxpool.NewWithConfig(context.Background(), configuredPool)
	if err != nil {
		return nil, fmt.Errorf("postgres: init pgxpool: %w", err)
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return TryWithAttemptsCtx(ctx, pool.Ping, RetryAttempts, RetryDelay)
		},
		OnStop: func(ctx context.Context) error {
			pool.Close()
			return nil
		},
	})

	log.Info("created postgres client")

	return pool, nil
}

// TryWithAttempts tries to get non-error result of calling function f with delay.
func TryWithAttempts(f func() error, attempts uint, delay time.Duration) (err error) {
	err = f()

	if err == nil {
		return nil
	}

	for i := uint(1); i < attempts; i++ {
		if err = f(); err == nil {
			return nil
		}
		zap.L().Warn("got error in attempter", zap.Uint("attempts", i+1), zap.NamedError("error", err))
		time.Sleep(delay)
	}
	return err
}

// TryWithAttemptsCtx is helper function that calls TryWithAttempts with function f transformed to closure that does not
// require ctx as necessary argument.
func TryWithAttemptsCtx(ctx context.Context, f func(context.Context) error, attempts uint, delay time.Duration) (err error) {
	return TryWithAttempts(func() error {
		return f(ctx)
	}, attempts, delay)
}
