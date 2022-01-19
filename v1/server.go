package machinery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"

	"github.com/RichardKnop/machinery/v1/backends/result"
	"github.com/RichardKnop/machinery/v1/brokers/eager"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/RichardKnop/machinery/v1/tracing"
	"github.com/RichardKnop/machinery/v1/utils"

	backendsiface "github.com/RichardKnop/machinery/v1/backends/iface"
	brokersiface "github.com/RichardKnop/machinery/v1/brokers/iface"
	lockiface "github.com/RichardKnop/machinery/v1/locks/iface"
	opentracing "github.com/opentracing/opentracing-go"
)

// Server is the main Machinery object and stores all configuration
// All the tasks workers process are registered against the server
type Server struct {
	config            *config.Config
	registeredTasks   *sync.Map
	broker            brokersiface.Broker
	backend           backendsiface.Backend
	lock              lockiface.Lock
	scheduler         *cron.Cron
	prePublishHandler func(*tasks.Signature)
}

// NewServerWithBrokerBackend ...
func NewServerWithBrokerBackendLock(cnf *config.Config, brokerServer brokersiface.Broker, backendServer backendsiface.Backend, lock lockiface.Lock) *Server {
	// 将初始化一个server
	srv := &Server{
		config:          cnf,
		registeredTasks: new(sync.Map), // 初始化一个并发安全的map
		broker:          brokerServer,
		backend:         backendServer,
		lock:            lock,
		scheduler:       cron.New(), // 初始化一个定时任务调度器
	}

	// Run scheduler job
	// 让一个携程负责调用相关工作
	go srv.scheduler.Run()

	return srv
}

// NewServer creates Server instance
// 初始化服务
func NewServer(cnf *config.Config) (*Server, error) {
	// 生成broker对象(主要对broker的配置进行校验)
	broker, err := BrokerFactory(cnf)
	if err != nil {
		return nil, err
	}

	// Backend is optional so we ignore the error
	// 生成backend对象，由于backend是可选参数，因此这里忽略了错误
	backend, _ := BackendFactory(cnf)

	// Init lock  初始化锁
	lock, err := LockFactory(cnf)
	if err != nil {
		return nil, err
	}

	// 将上面生成的对象，写入我们需要初始化的配置中
	srv := NewServerWithBrokerBackendLock(cnf, broker, backend, lock)

	// init for eager-mode
	// 初始化 "渴望模式"
	eager, ok := broker.(eager.Mode)
	if ok {
		// we don't have to call worker.Launch in eager mode
		// 我们不必在"渴望模式"下调用worker.Launch
		eager.AssignWorker(srv.NewWorker("eager", 0))
	}

	return srv, nil
}

// NewWorker creates Worker instance
func (server *Server) NewWorker(consumerTag string, concurrency int) *Worker {
	return &Worker{
		server:      server,      // server 对象
		ConsumerTag: consumerTag, // 消费者的标签
		Concurrency: concurrency, // 并发数
		Queue:       "",
	}
}

// NewCustomQueueWorker creates Worker instance with Custom Queue
// NewCustomQueueWorker创建带有自定义队列的Worker实例。
func (server *Server) NewCustomQueueWorker(consumerTag string, concurrency int, queue string) *Worker {
	return &Worker{
		server:      server,      // server 对象
		ConsumerTag: consumerTag, // 消费者的标签
		Concurrency: concurrency, // 并发数
		Queue:       queue,       // 队列
	}
}

// GetBroker returns broker
// 获取我们已经new好的broker对象
func (server *Server) GetBroker() brokersiface.Broker {
	return server.broker
}

// SetBroker sets broker
// 对我们已经初始化好的broker进行替换
func (server *Server) SetBroker(broker brokersiface.Broker) {
	server.broker = broker
}

// GetBackend returns backend
// 获取我们已经new好的backend对象
func (server *Server) GetBackend() backendsiface.Backend {
	return server.backend
}

// SetBackend sets backend
// 对我们已经初始化好的backend进行替换
func (server *Server) SetBackend(backend backendsiface.Backend) {
	server.backend = backend
}

// GetConfig returns connection object
// 获取已初始化的配置
func (server *Server) GetConfig() *config.Config {
	return server.config
}

// SetConfig sets config
// 设置新的配置
func (server *Server) SetConfig(cnf *config.Config) {
	server.config = cnf
}

// SetPreTaskHandler Sets pre publish handler
// 设置预处理程序
func (server *Server) SetPreTaskHandler(handler func(*tasks.Signature)) {
	server.prePublishHandler = handler
}

// RegisterTasks registers all tasks at once
// 一次注册所有任务
func (server *Server) RegisterTasks(namedTaskFuncs map[string]interface{}) error {
	// 遍历校验任务
	for _, task := range namedTaskFuncs {
		if err := tasks.ValidateTask(task); err != nil {
			return err
		}
	}

	// 将任务存储到registeredTasks
	for k, v := range namedTaskFuncs {
		server.registeredTasks.Store(k, v)
	}

	// 设置注册好的任务名称(将)
	server.broker.SetRegisteredTaskNames(server.GetRegisteredTaskNames())
	return nil
}

// RegisterTask registers a single task
// 注册单个任务
func (server *Server) RegisterTask(name string, taskFunc interface{}) error {
	// 校验任务合法性
	if err := tasks.ValidateTask(taskFunc); err != nil {
		return err
	}
	// 存储到registeredTasks
	server.registeredTasks.Store(name, taskFunc)
	// 将任务名称也放入registeredTasks
	server.broker.SetRegisteredTaskNames(server.GetRegisteredTaskNames())
	return nil
}

// IsTaskRegistered returns true if the task name is registered with this broker
// 如果此任务名称成功注册到broker将会返回true
func (server *Server) IsTaskRegistered(name string) bool {
	// 去registeredTasks中查看是否存在
	_, ok := server.registeredTasks.Load(name)
	return ok
}

// GetRegisteredTask returns registered task by name
// 通过任务名称获取任务
func (server *Server) GetRegisteredTask(name string) (interface{}, error) {
	taskFunc, ok := server.registeredTasks.Load(name)
	if !ok {
		return nil, fmt.Errorf("Task not registered error: %s", name)
	}
	return taskFunc, nil
}

// SendTaskWithContext will inject the trace context in the signature headers before publishing it
// SendTaskWithContext将在发布前在签名头中注入跟踪上下文。
func (server *Server) SendTaskWithContext(ctx context.Context, signature *tasks.Signature) (*result.AsyncResult, error) {
	// 一种命名的、定时的操作，表示工作流的一部分。Spans接受key:value标签以及附加到特定Span实例的细粒度、带时间戳的结构化日志（OpenTracing API ）
	span, _ := opentracing.StartSpanFromContext(ctx, "SendTask", tracing.ProducerOption(), tracing.MachineryTag)
	defer span.Finish()

	// tag the span with some info about the signature
	// 用一些关于签名的信息来标记跨度
	signature.Headers = tracing.HeadersWithSpan(signature.Headers, span)

	// Make sure result backend is defined
	// 确保结果后端被定义
	if server.backend == nil {
		return nil, errors.New("Result backend required")
	}

	// Auto generate a UUID if not set already
	// 如果没有设置，自动生成UUID
	if signature.UUID == "" {
		taskID := uuid.New().String()
		signature.UUID = fmt.Sprintf("task_%v", taskID)
	}

	// Set initial task state to PENDING
	// 将初始任务状态设置为待定
	if err := server.backend.SetStatePending(signature); err != nil {
		return nil, fmt.Errorf("Set state pending error: %s", err)
	}

	// 执行于处理函数
	if server.prePublishHandler != nil {
		server.prePublishHandler(signature)
	}

	// 任务发布(写入存储介质)
	if err := server.broker.Publish(ctx, signature); err != nil {
		return nil, fmt.Errorf("Publish message error: %s", err)
	}

	// 创建一个异步结果对象
	return result.NewAsyncResult(signature, server.backend), nil
}

// SendTask publishes a task to the default queue
// 将任务推到默认队列中
func (server *Server) SendTask(signature *tasks.Signature) (*result.AsyncResult, error) {
	return server.SendTaskWithContext(context.Background(), signature)
}

// SendChainWithContext will inject the trace context in all the signature headers before publishing it
// SendChainWithContext将在发布前在所有签名头中注入跟踪上下文。
func (server *Server) SendChainWithContext(ctx context.Context, chain *tasks.Chain) (*result.ChainAsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChain", tracing.ProducerOption(), tracing.MachineryTag, tracing.WorkflowChainTag)
	defer span.Finish()
	// 在跨度上标记一些有关链的信息
	tracing.AnnotateSpanWithChainInfo(span, chain)

	return server.SendChain(chain)
}

// SendChain triggers a chain of tasks
// SendChain 触发了一个任务链
func (server *Server) SendChain(chain *tasks.Chain) (*result.ChainAsyncResult, error) {
	_, err := server.SendTask(chain.Tasks[0])
	if err != nil {
		return nil, err
	}

	return result.NewChainAsyncResult(chain.Tasks, server.backend), nil
}

// SendGroupWithContext will inject the trace context in all the signature headers before publishing it
// SendGroupWithContext将在发布前在所有签名头中注入跟踪上下文。
func (server *Server) SendGroupWithContext(ctx context.Context, group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendGroup", tracing.ProducerOption(), tracing.MachineryTag, tracing.WorkflowGroupTag)
	defer span.Finish()

	// 标记关于分组的信息
	tracing.AnnotateSpanWithGroupInfo(span, group, sendConcurrency)

	// Make sure result backend is defined
	// 确保结果后端被定义
	if server.backend == nil {
		return nil, errors.New("Result backend required")
	}

	asyncResults := make([]*result.AsyncResult, len(group.Tasks))

	var wg sync.WaitGroup
	wg.Add(len(group.Tasks))
	errorsChan := make(chan error, len(group.Tasks)*2)

	// Init group
	server.backend.InitGroup(group.GroupUUID, group.GetUUIDs())

	// Init the tasks Pending state first
	// 将初始任务状态设置为待定
	for _, signature := range group.Tasks {
		if err := server.backend.SetStatePending(signature); err != nil {
			errorsChan <- err
			continue
		}
	}

	// 并发池子
	pool := make(chan struct{}, sendConcurrency)
	go func() {
		for i := 0; i < sendConcurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for i, signature := range group.Tasks {

		if sendConcurrency > 0 {
			<-pool
		}

		go func(s *tasks.Signature, index int) {
			defer wg.Done()

			// Publish task
			// 将任务存储到broker
			err := server.broker.Publish(ctx, s)

			if sendConcurrency > 0 {
				pool <- struct{}{}
			}

			if err != nil {
				errorsChan <- fmt.Errorf("Publish message error: %s", err)
				return
			}

			asyncResults[index] = result.NewAsyncResult(s, server.backend)
		}(signature, i)
	}

	done := make(chan int)
	go func() {
		wg.Wait()
		done <- 1
	}()

	select {
	case err := <-errorsChan:
		return asyncResults, err
	case <-done:
		return asyncResults, nil
	}
}

// SendGroup triggers a group of parallel tasks
// SendGroup 触发一组并行任务
func (server *Server) SendGroup(group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error) {
	return server.SendGroupWithContext(context.Background(), group, sendConcurrency)
}

// SendChordWithContext will inject the trace context in all the signature headers before publishing it
// SendChordWithContext将在发布前在所有签名头中注入跟踪上下文。
func (server *Server) SendChordWithContext(ctx context.Context, chord *tasks.Chord, sendConcurrency int) (*result.ChordAsyncResult, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChord", tracing.ProducerOption(), tracing.MachineryTag, tracing.WorkflowChordTag)
	defer span.Finish()

	// 在跨度上标记一些有chord的信息
	tracing.AnnotateSpanWithChordInfo(span, chord, sendConcurrency)

	_, err := server.SendGroupWithContext(ctx, chord.Group, sendConcurrency)
	if err != nil {
		return nil, err
	}

	return result.NewChordAsyncResult(
		chord.Group.Tasks,
		chord.Callback,
		server.backend,
	), nil
}

// SendChord triggers a group of parallel tasks with a callback
// SendChord 触发了一组带有回调的并行任务
func (server *Server) SendChord(chord *tasks.Chord, sendConcurrency int) (*result.ChordAsyncResult, error) {
	return server.SendChordWithContext(context.Background(), chord, sendConcurrency)
}

// GetRegisteredTaskNames returns slice of registered task names
// GetRegisteredTaskNames 以切片方式返回所有注册的任务名称
func (server *Server) GetRegisteredTaskNames() []string {
	taskNames := make([]string, 0)
	// 从registeredTasks种获取
	server.registeredTasks.Range(func(key, value interface{}) bool {
		taskNames = append(taskNames, key.(string))
		return true
	})
	return taskNames
}

// RegisterPeriodicTask register a periodic task which will be triggered periodically
// RegisterPeriodicTask 注册一个周期性任务，该任务将被周期性触发
func (server *Server) RegisterPeriodicTask(spec, name string, signature *tasks.Signature) error {
	//check spec
	// 检查提交的是否是crontab规范
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	// 定时任务执行的方法(写入)
	f := func() {
		//get lock
		err := server.lock.LockWithRetries(utils.GetLockName(name, spec), schedule.Next(time.Now()).UnixNano()-1)
		if err != nil {
			return
		}

		//send task
		_, err = server.SendTask(tasks.CopySignature(signature))
		if err != nil {
			log.ERROR.Printf("periodic task failed. task name is: %s. error is %s", name, err.Error())
		}
	}

	_, err = server.scheduler.AddFunc(spec, f)
	return err
}

// RegisterPeriodicChain register a periodic chain which will be triggered periodically
//RegisterPeriodicChain 注册一个将周期性触发的周期性链
func (server *Server) RegisterPeriodicChain(spec, name string, signatures ...*tasks.Signature) error {
	//check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	f := func() {
		// new chain
		chain, _ := tasks.NewChain(tasks.CopySignatures(signatures...)...)

		//get lock
		err := server.lock.LockWithRetries(utils.GetLockName(name, spec), schedule.Next(time.Now()).UnixNano()-1)
		if err != nil {
			return
		}

		//send task
		_, err = server.SendChain(chain)
		if err != nil {
			log.ERROR.Printf("periodic task failed. task name is: %s. error is %s", name, err.Error())
		}
	}

	_, err = server.scheduler.AddFunc(spec, f)
	return err
}

// RegisterPeriodicGroup register a periodic group which will be triggered periodically
func (server *Server) RegisterPeriodicGroup(spec, name string, sendConcurrency int, signatures ...*tasks.Signature) error {
	//check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	f := func() {
		// new group
		group, _ := tasks.NewGroup(tasks.CopySignatures(signatures...)...)

		//get lock
		err := server.lock.LockWithRetries(utils.GetLockName(name, spec), schedule.Next(time.Now()).UnixNano()-1)
		if err != nil {
			return
		}

		//send task
		_, err = server.SendGroup(group, sendConcurrency)
		if err != nil {
			log.ERROR.Printf("periodic task failed. task name is: %s. error is %s", name, err.Error())
		}
	}

	_, err = server.scheduler.AddFunc(spec, f)
	return err
}

// RegisterPeriodicChord register a periodic chord which will be triggered periodically
func (server *Server) RegisterPeriodicChord(spec, name string, sendConcurrency int, callback *tasks.Signature, signatures ...*tasks.Signature) error {
	//check spec
	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return err
	}

	f := func() {
		// new chord
		group, _ := tasks.NewGroup(tasks.CopySignatures(signatures...)...)
		chord, _ := tasks.NewChord(group, tasks.CopySignature(callback))

		//get lock
		err := server.lock.LockWithRetries(utils.GetLockName(name, spec), schedule.Next(time.Now()).UnixNano()-1)
		if err != nil {
			return
		}

		//send task
		_, err = server.SendChord(chord, sendConcurrency)
		if err != nil {
			log.ERROR.Printf("periodic task failed. task name is: %s. error is %s", name, err.Error())
		}
	}

	_, err = server.scheduler.AddFunc(spec, f)
	return err
}
