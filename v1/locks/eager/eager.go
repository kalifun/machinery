package eager

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrEagerLockFailed = errors.New("eager lock: failed to acquire lock")
)

type Lock struct {
	retries  int	// 重试次数
	interval time.Duration	// 重试等待时间
	register struct {
		sync.RWMutex
		m map[string]int64
	} 
}

// new 一个lock对象
func New() *Lock {
	return &Lock{
		retries:  3,
		interval: 5 * time.Second,
		register: struct {
			sync.RWMutex
			m map[string]int64
		}{
			m: make(map[string]int64),
		},
	}
}

// 重试获取锁
func (e *Lock) LockWithRetries(key string, value int64) error {
	for i := 0; i <= e.retries; i++ {
		err := e.Lock(key, value)
		if err == nil {
			//成功拿到锁，返回
			return nil
		}

		time.Sleep(e.interval)
	}
	return ErrEagerLockFailed
}

// 重试从map种获取 如果不存在或时间不一致则写入
func (e *Lock) Lock(key string, value int64) error {
	e.register.Lock()
	defer e.register.Unlock()
	timeout, exist := e.register.m[key]
	if !exist || time.Now().UnixNano() > timeout {
		e.register.m[key] = value
		return nil
	}
	return ErrEagerLockFailed
}
