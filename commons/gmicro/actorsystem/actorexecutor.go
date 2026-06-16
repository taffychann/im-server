package actorsystem

import (
	"context"
	"sync"

	"im-server/commons/gmicro/utils"

	"github.com/Jeffail/tunny"
)

type IExecutor interface {
	Execute(req *MessageRequest, msgSender *MsgSender)
}

type ActorExecutor struct {
	wraperChan  chan wraper
	executePool *tunny.Pool
	actorPool   sync.Pool // Actor 有临时状态，使用 sync.Pool 隔离实例，避免并发数据竞争。
}

func NewActorExecutorWithDefaultPool(pool *tunny.Pool, actorCreateFun func() IUntypedActor) *ActorExecutor {
	executor := &ActorExecutor{
		wraperChan:  make(chan wraper, buffersize),
		executePool: pool,
		actorPool: sync.Pool{
			New: func() any {
				return actorCreateFun()
			},
		},
	}
	go actorExecute(executor)
	return executor
}

func NewActorExecutor(concurrentCount int, actorCreateFun func() IUntypedActor) *ActorExecutor {
	executor := &ActorExecutor{
		wraperChan:  make(chan wraper, buffersize),
		executePool: tunny.NewCallback(concurrentCount),
		actorPool: sync.Pool{
			New: func() interface{} {
				return actorCreateFun()
			},
		},
	}
	go actorExecute(executor)
	return executor
}

func (executor *ActorExecutor) Execute(req *MessageRequest, msgSender *MsgSender) {
	actorObj := executor.actorPool.Get()
	executor.wraperChan <- commonExecute(req, msgSender, actorObj)
	executor.actorPool.Put(actorObj)
}

func actorExecute(executor *ActorExecutor) {
	for { // 阻塞循环，等待消息请求
		wraper := <-executor.wraperChan
		go executor.executePool.Process(func() { // tunny仅支持有限数量同步执行，使用goroutine包装异步执行
			defer utils.Recovery()

			actorObj := executor.actorPool.Get()

			senderHandler, ok := actorObj.(ISenderHandler)
			if ok {
				senderHandler.SetSender(wraper.sender)
			}

			receiveHandler, ok := actorObj.(IReceiveHandler)
			if ok {
				receiveHandler.OnReceive(context.Background(), wraper.msg) // 实际的业务处理逻辑
			}
			executor.actorPool.Put(actorObj)
		})
	}
}
