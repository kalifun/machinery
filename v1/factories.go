package machinery

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	neturl "net/url"

	"github.com/RichardKnop/machinery/v1/config"

	amqpbroker "github.com/RichardKnop/machinery/v1/brokers/amqp"
	eagerbroker "github.com/RichardKnop/machinery/v1/brokers/eager"
	gcppubsubbroker "github.com/RichardKnop/machinery/v1/brokers/gcppubsub"
	brokeriface "github.com/RichardKnop/machinery/v1/brokers/iface"
	redisbroker "github.com/RichardKnop/machinery/v1/brokers/redis"
	sqsbroker "github.com/RichardKnop/machinery/v1/brokers/sqs"

	amqpbackend "github.com/RichardKnop/machinery/v1/backends/amqp"
	dynamobackend "github.com/RichardKnop/machinery/v1/backends/dynamodb"
	eagerbackend "github.com/RichardKnop/machinery/v1/backends/eager"
	backendiface "github.com/RichardKnop/machinery/v1/backends/iface"
	memcachebackend "github.com/RichardKnop/machinery/v1/backends/memcache"
	mongobackend "github.com/RichardKnop/machinery/v1/backends/mongo"
	nullbackend "github.com/RichardKnop/machinery/v1/backends/null"
	redisbackend "github.com/RichardKnop/machinery/v1/backends/redis"

	eagerlock "github.com/RichardKnop/machinery/v1/locks/eager"
	lockiface "github.com/RichardKnop/machinery/v1/locks/iface"
	redislock "github.com/RichardKnop/machinery/v1/locks/redis"
)

// BrokerFactory creates a new object of iface.Broker
// BrokerFactory创建了一个iface.Broker的新对象。
// Currently only AMQP/S broker is supported
// 目前只支持AMQP/S代理
func BrokerFactory(cnf *config.Config) (brokeriface.Broker, error) {
	// 判断broker配置前缀是否是"amqp://"
	if strings.HasPrefix(cnf.Broker, "amqp://") {
		return amqpbroker.New(cnf), nil
	}

	// 判断broker配置前缀是否是"amqps://"
	if strings.HasPrefix(cnf.Broker, "amqps://") {
		return amqpbroker.New(cnf), nil
	}

	// 判断broker配置是否是redis且采用的是"redis://"或"rediss://"
	if strings.HasPrefix(cnf.Broker, "redis://") || strings.HasPrefix(cnf.Broker, "rediss://") {
		var scheme string
		if strings.HasPrefix(cnf.Broker, "redis://") {
			scheme = "redis://"
		} else {
			scheme = "rediss://"
		}
		parts := strings.Split(cnf.Broker, scheme)
		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"Redis broker connection string should be in format %shost:port, instead got %s", scheme,
				cnf.Broker,
			)
		}
		brokers := strings.Split(parts[1], ",")
		if len(brokers) > 1 || (cnf.Redis != nil && cnf.Redis.ClusterMode) {
			return redisbroker.NewGR(cnf, brokers, 0), nil
		} else {
			// 解析redis的配置
			redisHost, redisPassword, redisDB, err := ParseRedisURL(cnf.Broker)
			if err != nil {
				return nil, err
			}
			return redisbroker.New(cnf, redisHost, redisPassword, "", redisDB), nil
		}
	}

	// 如果是redis+socket形式则走这部分逻辑
	if strings.HasPrefix(cnf.Broker, "redis+socket://") {
		redisSocket, redisPassword, redisDB, err := ParseRedisSocketURL(cnf.Broker)
		if err != nil {
			return nil, err
		}

		return redisbroker.New(cnf, "", redisPassword, redisSocket, redisDB), nil
	}

	// 采用的是内存方式
	if strings.HasPrefix(cnf.Broker, "eager") {
		return eagerbroker.New(), nil
	}

	// 亚马逊的sqs消息队列服务
	if _, ok := os.LookupEnv("DISABLE_STRICT_SQS_CHECK"); ok {
		//disable SQS name check, so that users can use this with local simulated SQS
		//禁用SQS名称检查，这样用户就可以用本地模拟的SQS使用。
		//where sql broker url might not start with https://sqs
		//其中sql broker url可能不是以https://sqs 开始的。

		//even when disabling strict SQS naming check, make sure its still a valid http URL
		//即使在禁用严格的SQS命名检查时，也要确保它仍然是一个有效的http URL
		if strings.HasPrefix(cnf.Broker, "https://") || strings.HasPrefix(cnf.Broker, "http://") {
			return sqsbroker.New(cnf), nil
		}
	} else {
		if strings.HasPrefix(cnf.Broker, "https://sqs") {
			return sqsbroker.New(cnf), nil
		}
	}

	// Google的消息队列服务
	if strings.HasPrefix(cnf.Broker, "gcppubsub://") {
		projectID, subscriptionName, err := ParseGCPPubSubURL(cnf.Broker)
		if err != nil {
			return nil, err
		}
		return gcppubsubbroker.New(cnf, projectID, subscriptionName)
	}

	// 如果是其他配置则返回错误
	return nil, fmt.Errorf("Factory failed with broker URL: %v", cnf.Broker)
}

// BackendFactory creates a new object of backends.Interface
// BackendFactory创建一个backends.Interface的新对象。
// Currently supported backends are AMQP/S and Memcache
// 目前支持的backends是AMQP/S和Memcache
func BackendFactory(cnf *config.Config) (backendiface.Backend, error) {
	// amqp协议
	if strings.HasPrefix(cnf.ResultBackend, "amqp://") {
		return amqpbackend.New(cnf), nil
	}
	// amsps协议
	if strings.HasPrefix(cnf.ResultBackend, "amqps://") {
		return amqpbackend.New(cnf), nil
	}

	// memcache
	if strings.HasPrefix(cnf.ResultBackend, "memcache://") {
		parts := strings.Split(cnf.ResultBackend, "memcache://")
		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"Memcache result backend connection string should be in format memcache://server1:port,server2:port, instead got %s",
				cnf.ResultBackend,
			)
		}
		servers := strings.Split(parts[1], ",")
		return memcachebackend.New(cnf, servers), nil
	}

	// redis
	if strings.HasPrefix(cnf.ResultBackend, "redis://") || strings.HasPrefix(cnf.ResultBackend, "rediss://") {
		var scheme string
		if strings.HasPrefix(cnf.ResultBackend, "redis://") {
			scheme = "redis://"
		} else {
			scheme = "rediss://"
		}
		parts := strings.Split(cnf.ResultBackend, scheme)
		addrs := strings.Split(parts[1], ",")
		if len(addrs) > 1 || (cnf.Redis != nil && cnf.Redis.ClusterMode) {
			return redisbackend.NewGR(cnf, addrs, 0), nil
		} else {
			redisHost, redisPassword, redisDB, err := ParseRedisURL(cnf.ResultBackend)

			if err != nil {
				return nil, err
			}

			return redisbackend.New(cnf, redisHost, redisPassword, "", redisDB), nil
		}
	}

	// redis + socket
	if strings.HasPrefix(cnf.ResultBackend, "redis+socket://") {
		redisSocket, redisPassword, redisDB, err := ParseRedisSocketURL(cnf.ResultBackend)
		if err != nil {
			return nil, err
		}

		return redisbackend.New(cnf, "", redisPassword, redisSocket, redisDB), nil
	}

	// mongodb
	if strings.HasPrefix(cnf.ResultBackend, "mongodb://") ||
		strings.HasPrefix(cnf.ResultBackend, "mongodb+srv://") {
		return mongobackend.New(cnf)
	}

	// 内存
	if strings.HasPrefix(cnf.ResultBackend, "eager") {
		return eagerbackend.New(), nil
	}

	// 不使用
	if strings.HasPrefix(cnf.ResultBackend, "null") {
		return nullbackend.New(), nil
	}

	// dynamodb(亚马逊云数据库)
	if strings.HasPrefix(cnf.ResultBackend, "https://dynamodb") {
		return dynamobackend.New(cnf), nil
	}

	// 不包含以上
	return nil, fmt.Errorf("Factory failed with result backend: %v", cnf.ResultBackend)
}

// ParseRedisURL ...
func ParseRedisURL(url string) (host, password string, db int, err error) {
	// redis://pwd@host/db

	var u *neturl.URL
	u, err = neturl.Parse(url)
	if err != nil {
		return
	}
	if u.Scheme != "redis" && u.Scheme != "rediss" {
		err = errors.New("No redis scheme found")
		return
	}

	if u.User != nil {
		var exists bool
		password, exists = u.User.Password()
		if !exists {
			password = u.User.Username()
		}
	}

	host = u.Host

	parts := strings.Split(u.Path, "/")
	if len(parts) == 1 {
		db = 0 //default redis db
	} else {
		db, err = strconv.Atoi(parts[1])
		if err != nil {
			db, err = 0, nil //ignore err here
		}
	}

	return
}

// LockFactory creates a new object of iface.Lock
// LockFactory创建一个iface.Lock的新对象。
// Currently supported lock is redis
// 目前支持的lock是redis
func LockFactory(cnf *config.Config) (lockiface.Lock, error) {
	// 如果用户使用的是"eager",则采用eagerlock
	if strings.HasPrefix(cnf.Lock, "eager") {
		return eagerlock.New(), nil
	}
	// 注：此处是redis的lock
	if strings.HasPrefix(cnf.Lock, "redis://") {
		parts := strings.Split(cnf.Lock, "redis://")
		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"Redis broker connection string should be in format redis://host:port, instead got %s",
				cnf.Lock,
			)
		}
		locks := strings.Split(parts[1], ",")
		return redislock.New(cnf, locks, 0, 3), nil
	}

	// Lock is required for periodic tasks to work, therefor return in memory lock in case none is configured
	// 周期性任务的工作需要锁，因此在没有配置锁的情况下，返回内存中的锁。
	return eagerlock.New(), nil
}

// ParseRedisSocketURL extracts Redis connection options from a URL with the
// redis+socket:// scheme. This scheme is not standard (or even de facto) and
// is used as a transitional mechanism until the the config package gains the
// proper facilities to support socket-based connections.
func ParseRedisSocketURL(url string) (path, password string, db int, err error) {
	parts := strings.Split(url, "redis+socket://")
	if parts[0] != "" {
		err = errors.New("No redis scheme found")
		return
	}

	// redis+socket://password@/path/to/file.soc:/db

	if len(parts) != 2 {
		err = fmt.Errorf("Redis socket connection string should be in format redis+socket://password@/path/to/file.sock:/db, instead got %s", url)
		return
	}

	remainder := parts[1]

	// Extract password if any
	parts = strings.SplitN(remainder, "@", 2)
	if len(parts) == 2 {
		password = parts[0]
		remainder = parts[1]
	} else {
		remainder = parts[0]
	}

	// Extract path
	parts = strings.SplitN(remainder, ":", 2)
	path = parts[0]
	if path == "" {
		err = fmt.Errorf("Redis socket connection string should be in format redis+socket://password@/path/to/file.sock:/db, instead got %s", url)
		return
	}
	if len(parts) == 2 {
		remainder = parts[1]
	}

	// Extract DB if any
	parts = strings.SplitN(remainder, "/", 2)
	if len(parts) == 2 {
		db, _ = strconv.Atoi(parts[1])
	}

	return
}

// ParseGCPPubSubURL Parse GCP Pub/Sub URL
// url: gcppubsub://YOUR_GCP_PROJECT_ID/YOUR_PUBSUB_SUBSCRIPTION_NAME
func ParseGCPPubSubURL(url string) (string, string, error) {
	parts := strings.Split(url, "gcppubsub://")
	if parts[0] != "" {
		return "", "", errors.New("No gcppubsub scheme found")
	}

	if len(parts) != 2 {
		return "", "", fmt.Errorf("gcppubsub scheme should be in format gcppubsub://YOUR_GCP_PROJECT_ID/YOUR_PUBSUB_SUBSCRIPTION_NAME, instead got %s", url)
	}

	remainder := parts[1]

	parts = strings.Split(remainder, "/")
	if len(parts) == 2 {
		if parts[0] == "" {
			return "", "", fmt.Errorf("gcppubsub scheme should be in format gcppubsub://YOUR_GCP_PROJECT_ID/YOUR_PUBSUB_SUBSCRIPTION_NAME, instead got %s", url)
		}
		if parts[1] == "" {
			return "", "", fmt.Errorf("gcppubsub scheme should be in format gcppubsub://YOUR_GCP_PROJECT_ID/YOUR_PUBSUB_SUBSCRIPTION_NAME, instead got %s", url)
		}
		return parts[0], parts[1], nil
	}

	return "", "", fmt.Errorf("gcppubsub scheme should be in format gcppubsub://YOUR_GCP_PROJECT_ID/YOUR_PUBSUB_SUBSCRIPTION_NAME, instead got %s", url)
}
