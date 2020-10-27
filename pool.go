package pool

import (
	"fmt"
	"sync"
	"sync/atomic"
	"github.com/jpetersdev/limiter"
	"time"
)

const (
	add    = "add"
	remove = "remove"
	stop   = "stop"
	pause  = "pause"
)

type Pool struct {
	paused     		chan struct{}
	finished   		chan struct{}
	action     		chan string
	statusChan   	chan Status
	availableChan   chan int64
	status			string
	workerStatus	map[int64]string
	running    		int64
	threads    		int64
	task       		Task
	limiter         *limiter.Limiter
	wg		  		sync.WaitGroup
}

type Task interface {
	Complete() bool
	Run(id int64, s chan <- Status) error
}

type Status struct {
	Id		int64
	Status	string
}

func NewWorkerPool(task Task, MaxWorkers int) (Pool, error) {
	l, err := limiter.NewLimiter(int64(MaxWorkers), 0)
	if err != nil { return Pool{}, err }
	return Pool{
		paused:        make(chan struct{}),
		finished:      make(chan struct{}),
		action:        make(chan string),
		statusChan:    make(chan Status, MaxWorkers),
		availableChan: make(chan int64, MaxWorkers),
		status:       "Idle",
		workerStatus: map[int64]string{},
		running:      0,
		threads:      0,
		task:         task,
		limiter:      l,
		wg:           sync.WaitGroup{},
	}, nil
}

func (pool *Pool) Start(threads int) {
	go pool.handler()
	go pool.deletgator()
	pool.status = "Starting"
	pool.Add(threads)
	pool.status = "Running"
}

func (pool *Pool) Pause() {
	pool.status = "Pausing"
	pool.action <- pause
	<- pool.paused
	pool.status = "Paused"
}

func (pool *Pool) Resume(threads int) {
	pool.status = "Resuming"
	pool.Add(threads)
	pool.status = "Running"
}

func (pool *Pool) Stop() {
	pool.status = "Stopping"
	pool.action <- stop
	<- pool.finished
	pool.status = "Stopped"
}

func (pool *Pool) Add(n int) {
	if n < 0 { n = n * -1 } else if n == 0 { return }
	for i := 0; i < n; i++ {
		pool.action <- add
	}
}

func (pool *Pool) Remove(n int) {
	if n < 0 { n = n * -1 } else if n == 0 { return }
	for i := 0; i < n; i++ {
		pool.action <- remove
	}
}

func (pool *Pool) Running() int {
	return int(pool.running)
}

func (pool *Pool) Stats() map[int64]string {
	return pool.workerStatus
}

func (pool *Pool) Done() {
	<- pool.finished
}

func (pool *Pool) handler() {
	t := time.NewTicker(time.Second * 5)
	for {
		select {
		case action := <-pool.action:
			switch action {
			case add:
				pool.addThread()
			case remove:
				threads := int(atomic.LoadInt64(&pool.threads))
				if threads <= 0 { break }
				pool.removeThread()
			case stop:
				threads := int(atomic.LoadInt64(&pool.threads))
				for i := 0; i < threads; i++ {
					pool.removeThread()
				}
				pool.wg.Wait()
				pool.finished <- struct{}{}
			case pause:
				threads := int(atomic.LoadInt64(&pool.threads))
				for i := 0; i < threads; i++ {
					pool.removeThread()
				}
				pool.wg.Wait()
				pool.paused <- struct{}{}
			}
		case stat := <- pool.statusChan:
			pool.workerStatus[stat.Id] = stat.Status
		case <-t.C:
			if pool.task.Complete() && pool.running == 0 {
				fmt.Println("waiting")
				pool.wg.Wait()
				fmt.Println("done waiting")
				pool.status = "Done"
				pool.finished <- struct{}{}
			}
		}
	}
}

func (pool *Pool) addThread() {
	if err := pool.limiter.Increment(); err == nil {
		atomic.AddInt64(&pool.threads, 1)
		pool.availableChan <- pool.threads
	}
}

func (pool *Pool) removeThread() {
	if err := pool.limiter.Decrement(); err == nil {
		atomic.AddInt64(&pool.threads, -1)
	}
}

func (pool *Pool) deletgator() {
	for {
		if err := pool.limiter.Acquire(); err != nil || pool.task.Complete() {
			continue
		}
		go func() {
			id := <- pool.availableChan
			if id > pool.threads {
				pool.limiter.Release()
				return
			}
			pool.wg.Add(1)
			atomic.AddInt64(&pool.running, 1)
			pool.statusChan <- Status{Id: id, Status: "Starting"}
			defer func(){
				if err := recover(); err != nil {
					pool.statusChan <- Status{Id: id, Status: fmt.Sprintf("unhandled error occurred: %v", err),}
				}

				atomic.AddInt64(&pool.running, -1)
				pool.wg.Done()
				pool.limiter.Release()
				pool.availableChan <- id
			}()
			if err := pool.task.Run(id, pool.statusChan); err != nil {
				pool.statusChan <- Status{Id: id, Status: fmt.Sprintf("unhandled error occurred: %v", err),}
			}
		}()
	}
}