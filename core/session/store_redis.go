package session

import (
	"context"
	"time"

	storageredis "github.com/cshekharsharma/photon/storage/redis"
)

var (
	redisSetConfig        = storageredis.SetConnectionConfig
	redisConnect          = storageredis.Connect
	redisConnectorFactory = func() storageredis.RedisConnectorInterface { return &storageredis.RedisConnector{} }
	redisClientFromConn   = func(conn storageredis.RedisInterface) storageredis.RedisClientInterface { return conn.GetClient() }
	redisIsNil            = storageredis.IsNil
)

func newRedisStore(cfg StoreOptions) (*redisStore, func() error, error) {
	redisSetConfig(cfg.Name, &storageredis.ConnectionConfig{
		Address:  cfg.Address,
		Username: cfg.Username,
		Password: cfg.Password,
		Database: cfg.DB,
		PoolSize: cfg.PoolSize,
	})

	conn, err := redisConnect(context.Background(), redisConnectorFactory(), cfg.Name)
	if err != nil {
		return nil, nil, err
	}

	client := redisClientFromConn(conn)

	if cfg.PingOnInit != nil && *cfg.PingOnInit {
		if err := client.Ping(context.Background()).Err(); err != nil {
			_ = conn.Close()
			return nil, nil, err
		}
	}

	return &redisStore{client: client, prefix: cfg.KeyPrefix}, conn.Close, nil
}

type redisStore struct {
	client storageredis.RedisClientInterface
	prefix string
}

func (s *redisStore) Find(token string) ([]byte, bool, error) {
	return s.FindCtx(context.Background(), token)
}

func (s *redisStore) FindCtx(ctx context.Context, token string) ([]byte, bool, error) {
	if token == "" {
		return nil, false, nil
	}

	cmd := s.client.Get(ctx, s.key(token))
	b, err := cmd.Bytes()
	if redisIsNil(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	return b, true, nil
}

func (s *redisStore) Commit(token string, b []byte, expiry time.Time) error {
	return s.CommitCtx(context.Background(), token, b, expiry)
}

func (s *redisStore) CommitCtx(ctx context.Context, token string, b []byte, expiry time.Time) error {
	if token == "" {
		return nil
	}

	ttl := expiryToTTL(expiry)
	return s.client.Set(ctx, s.key(token), b, ttl).Err()
}

func (s *redisStore) Delete(token string) error {
	return s.DeleteCtx(context.Background(), token)
}

func (s *redisStore) DeleteCtx(ctx context.Context, token string) error {
	if token == "" {
		return nil
	}
	return s.client.Del(ctx, s.key(token)).Err()
}

func (s *redisStore) Close() error {
	return s.client.Close()
}

func (s *redisStore) key(token string) string {
	if s.prefix == "" {
		return token
	}
	return s.prefix + token
}

func expiryToTTL(expiry time.Time) time.Duration {
	if expiry.IsZero() {
		return 0
	}

	ttl := time.Until(expiry)
	if ttl <= 0 {
		return time.Second
	}
	return ttl
}
