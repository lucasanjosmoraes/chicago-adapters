package redigoadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/lucasanjosmoraes/chicago-ports/pkg/cache"
	"github.com/lucasanjosmoraes/chicago-ports/pkg/log"
)

// ErrNil is retuned when no value is founded to the given key.
var ErrNil = errors.New("there's no value corresponding to the given key")

// Config stores all Adapter configuration variables.
type Config struct {
	Host         string
	Password     string
	Database     int
	TTL          int
	MaxIdleConn  int
	ReadTimeout  int
	WriteTimeout int
	TLS          bool
}

// Adapter is a Cacher implementation based on redigo dependency.
type Adapter struct {
	Config Config
	Logger log.Logger
	Pool   *redis.Pool
}

// New instantiates a new Adapter and also helps us to validate if it implements
// cache.Cacher.
func New(ctx context.Context, c Config, l log.Logger) cache.Cacher {
	return Adapter{
		Config: c,
		Logger: l,
		Pool: &redis.Pool{
			MaxIdle: c.MaxIdleConn,
			Dial:    dialFunc(ctx, c),
		},
	}
}

func dialFunc(ctx context.Context, c Config) func() (redis.Conn, error) {
	return func() (redis.Conn, error) {
		readTimeout := time.Duration(c.ReadTimeout) * time.Second
		writeTimeout := time.Duration(c.WriteTimeout) * time.Second
		conn, err := redis.DialContext(
			ctx,
			"tcp",
			c.Host,
			redis.DialDatabase(c.Database),
			redis.DialPassword(c.Password),
			redis.DialWriteTimeout(writeTimeout),
			redis.DialReadTimeout(readTimeout),
			redis.DialUseTLS(c.TLS),
		)
		if err != nil {
			return nil, err
		}

		return conn, nil
	}
}

// Read return a value for the given key, if it's stored.
func (a Adapter) Read(ctx context.Context, key string) ([]byte, error) {
	resChan := make(chan []byte)
	errChan := make(chan error)

	go func(k string) {
		s, err := redis.String(a.Pool.Get().Do("GET", key))
		if err == redis.ErrNil {
			errChan <- ErrNil
			return
		} else if err != nil {
			errChan <- err
			return
		}

		resChan <- []byte(s)
	}(key)

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context timeout reached trying to read key %s", key)
	case b := <-resChan:
		return b, nil
	case err := <-errChan:
		return nil, err
	}
}

// Store will store a given value associated with a given key.
func (a Adapter) Store(ctx context.Context, key string, value []byte) error {
	errChan := make(chan error)

	go func(k string, v []byte, exp int, timeout int) {
		_, err := a.Pool.Get().Do("SET", key, value, "EX", timeout)
		if err != nil {
			errChan <- err
			return
		}

		if exp == 0 {
			_, err = a.Pool.Get().Do("SET", key, value)
		} else {
			_, err = a.Pool.Get().Do("SET", key, value, "EX", exp)
		}

		errChan <- err
	}(key, value, a.Config.TTL, a.Config.WriteTimeout)

	select {
	case <-ctx.Done():
		return fmt.Errorf("context timeout reached trying to store key %s", key)
	case err := <-errChan:
		return err
	}
}

// GetKeys will retrieve all keys that contains the given pattern.
func (a Adapter) GetKeys(ctx context.Context, pattern string) ([]string, error) {
	resChan := make(chan []string)
	errChan := make(chan error)

	go func(p string) {
		keys := make([]string, 0)
		i := 0

		for {
			a, err := redis.Values(a.Pool.Get().Do("SCAN", i, "MATCH", p))
			if err == redis.ErrNil {
				resChan <- keys
				return
			}
			if err != nil {
				errChan <- fmt.Errorf("error getting keys that could match with pattern: %s", pattern)
				return
			}

			i, _ = redis.Int(a[0], nil)
			foundedKeys, _ := redis.Strings(a[1], nil)
			keys = append(keys, foundedKeys...)

			if i == 0 {
				break
			}
		}

		resChan <- keys
	}(pattern)

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context timeout reached trying to getting keys from pattern %s", pattern)
	case keys := <-resChan:
		return keys, nil
	case err := <-errChan:
		return nil, err
	}
}

// Delete will delete a value whose key exactly matches the given key.
func (a Adapter) Delete(ctx context.Context, key string) (int, error) {
	if len(key) == 0 {
		return 0, nil
	}

	resChan := make(chan int)
	errChan := make(chan error)

	go func(k string) {
		count, err := redis.Int(a.Pool.Get().Do("DEL", k))
		if err != nil {
			errChan <- err
			return
		}

		resChan <- count
	}(key)

	select {
	case <-ctx.Done():
		return 0, fmt.Errorf("context timeout reached trying to delete key %s", key)
	case count := <-resChan:
		return count, nil
	case err := <-errChan:
		return 0, err
	}
}

// DeleteMany will delete multiple values that exactly match the given keys, if any.
// The number of deleted items is returned.
func (a Adapter) DeleteMany(ctx context.Context, keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	resChan := make(chan int)
	errChan := make(chan error)

	go func(ks []string) {
		args := make([]interface{}, len(ks))
		for _, k := range ks {
			args = append(args, k)
		}

		count, err := redis.Int(a.Pool.Get().Do("DEL", args...))
		if err != nil {
			errChan <- err
			return
		}

		resChan <- count
	}(keys)

	select {
	case <-ctx.Done():
		return 0, fmt.Errorf("context timeout reached trying to delete %d keys", len(keys))
	case count := <-resChan:
		return count, nil
	case err := <-errChan:
		return 0, err
	}
}

// Stop finishes adapter and dispose its resources.
func (a Adapter) Stop(_ context.Context) error {
	return a.Pool.Close()
}

// StopError return the error generated by the context, if any.
func (a Adapter) StopError() error {
	return context.Canceled
}
