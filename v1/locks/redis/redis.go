package redis

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/go-redis/redis/v8"
)

var (
	ErrRedisLockFailed = errors.New("redis lock: failed to acquire lock")
)

type Lock struct {
	rclient  redis.UniversalClient	// redis client
	retries  int	// 重试次数
	interval time.Duration	// 重试等待时间
}

// new 一个lock对象
func New(cnf *config.Config, addrs []string, db, retries int) Lock {
	// 理论上应该加一个默认重试次数。直接返回又没有成功生成一个对象
	if retries <= 0 {
		return Lock{}
	}
	lock := Lock{retries: retries}

	var password string

	parts := strings.Split(addrs[0], "@")
	if len(parts) >= 2 {
		password = strings.Join(parts[:len(parts)-1], "@")
		addrs[0] = parts[len(parts)-1] // addr is the last one without @
	}

	ropt := &redis.UniversalOptions{
		Addrs:    addrs,
		DB:       db,
		Password: password,
	}
	if cnf.Redis != nil {
		ropt.MasterName = cnf.Redis.MasterName
	}

	lock.rclient = redis.NewUniversalClient(ropt)

	return lock
}

// 重试锁
func (r Lock) LockWithRetries(key string, unixTsToExpireNs int64) error {
	for i := 0; i <= r.retries; i++ {
		err := r.Lock(key, unixTsToExpireNs)
		if err == nil {
			//成功拿到锁，返回
			return nil
		}

		time.Sleep(r.interval)
	}
	return ErrRedisLockFailed
}

func (r Lock) Lock(key string, unixTsToExpireNs int64) error {
	// 计算一个过期时间
	now := time.Now().UnixNano()
	expiration := time.Duration(unixTsToExpireNs + 1 - now)
	ctx := r.rclient.Context()

	success, err := r.rclient.SetNX(ctx, key, unixTsToExpireNs, expiration).Result()
	if err != nil {
		return err
	}

	if !success {
		// 查看是否已经存在了此key
		v, err := r.rclient.Get(ctx, key).Result()
		if err != nil {
			return err
		}
		timeout, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		// 存储的时间不等于0且现在的时间大于了存储的时间则重新写入
		if timeout != 0 && now > int64(timeout) {
			newTimeout, err := r.rclient.GetSet(ctx, key, unixTsToExpireNs).Result()
			if err != nil {
				return err
			}

			curTimeout, err := strconv.Atoi(newTimeout)
			if err != nil {
				return err
			}

			if now > int64(curTimeout) {
				// success to acquire lock with get set
				// set the expiration of redis key
				r.rclient.Expire(ctx, key, expiration)
				return nil
			}

			return ErrRedisLockFailed
		}

		return ErrRedisLockFailed
	}

	return nil
}
