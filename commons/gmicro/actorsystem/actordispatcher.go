package actorsystem

import (
	"context"
	"sync"
	"time"

	"im-server/commons/gmicro/logs"
	"im-server/commons/gmicro/utils"

	"github.com/Jeffail/tunny"
	timewheel "github.com/rfyiamcool/go-timewheel"
	"google.golang.org/protobuf/proto"
)

const buffersize int = 8192

type ActorDispatcher struct {
	dispatchMap        sync.Map
	callbackMap        sync.Map
	msgSender          *MsgSender
	timer              *timewheel.TimeWheel
	callbackPool       *tunny.Pool
	callbackWraperChan chan wraper

	executorCommonPool *tunny.Pool
}

func NewActorDispatcher(sender *MsgSender) *ActorDispatcher {
	timer, err := timewheel.NewTimeWheel(1*time.Second, 360)
	if err != nil {
		logs.Error("error when start timewheel of dispatcher")
	}
	dispatcher := &ActorDispatcher{
		msgSender:          sender,
		timer:              timer,
		callbackPool:       tunny.NewCallback(buffersize),
		callbackWraperChan: make(chan wraper, buffersize),
		executorCommonPool: tunny.NewCallback(2 * buffersize),
	}
	timer.Start()
	go callbackActorExecute(dispatcher.callbackPool, dispatcher.callbackWraperChan)
	return dispatcher
}

// 把一条请求消息路由到正确的执行器，然后触发处理
func (dispatcher *ActorDispatcher) Dispatch(req *MessageRequest) {
	targetMethod := req.TarMethod
	var executor IExecutor

	if targetMethod == "" { //callback actor
		key := utils.Bytes2ShortString(req.Session)
		obj, ok := dispatcher.callbackMap.LoadAndDelete(key)
		if ok {
			callbackExecutor := obj.(*CallbackActorExecutor)
			//remove from timer task
			task := callbackExecutor.Task
			if task != nil {
				dispatcher.timer.Remove(task)
			}
			executor = callbackExecutor
		}
	} else {
		obj, ok := dispatcher.dispatchMap.Load(targetMethod) // 按消息目标方法名找对应 actor 执行器
		if ok {
			executor = obj.(IExecutor)
		}
	}
	if executor != nil {
		executor.Execute(req, dispatcher.msgSender)
	}
}

func (dispatcher *ActorDispatcher) Destroy() {
	if dispatcher.timer != nil {
		dispatcher.timer.Stop()
	}
}

// 使用 dispatcher 的公共执行池（executorCommonPool）创建并注册一个 ActorExecutor 到单个方法
func (dispatcher *ActorDispatcher) RegisterActor(method string, actorCreateFun func() IUntypedActor) {
	executor := NewActorExecutorWithDefaultPool(dispatcher.executorCommonPool, actorCreateFun)
	dispatcher.dispatchMap.Store(method, executor)
}

// 为单个方法注册一个独立的 ActorExecutor，允许指定并发数量
func (dispatcher *ActorDispatcher) RegisterStandaloneActor(method string, actorCreateFun func() IUntypedActor, concurrentCount int) {
	var executor *ActorExecutor
	if concurrentCount > 0 {
		executor = NewActorExecutor(concurrentCount, actorCreateFun)
	} else {
		executor = NewActorExecutorWithDefaultPool(dispatcher.executorCommonPool, actorCreateFun)
	}
	dispatcher.dispatchMap.Store(method, executor)
}

// 用公共执行池创建一个 ActorExecutor，并把同一个执行器实例注册到多个方法上
func (dispatcher *ActorDispatcher) RegisterMultiMethodActor(methods []string, actorCreateFun func() IUntypedActor) {
	executor := NewActorExecutorWithDefaultPool(dispatcher.executorCommonPool, actorCreateFun)
	for _, method := range methods {
		dispatcher.dispatchMap.Store(method, executor)
	}
}

// 为多个方法注册一个独立的 ActorExecutor，允许指定并发数量，并把同一个执行器实例注册到多个方法上
func (dispatcher *ActorDispatcher) RegisterStandaloneMultiMethodActor(methods []string, actorCreateFun func() IUntypedActor, concurrentCount int) {
	var executor *ActorExecutor
	if concurrentCount > 0 {
		executor = NewActorExecutor(concurrentCount, actorCreateFun)
	} else {
		executor = NewActorExecutorWithDefaultPool(dispatcher.executorCommonPool, actorCreateFun)
	}
	for _, method := range methods {
		dispatcher.dispatchMap.Store(method, executor)
	}
}

func (dispatcher *ActorDispatcher) AddCallbackActor(session []byte, actor ICallbackUntypedActor, ttl time.Duration) {
	executor := NewCallbackActorExecutor(dispatcher.callbackPool, dispatcher.callbackWraperChan, actor)
	key := utils.Bytes2ShortString(session)
	dispatcher.callbackMap.Store(key, executor)
	task := dispatcher.timer.Add(ttl, func() {
		obj, ok := dispatcher.callbackMap.LoadAndDelete(key)
		if ok {
			executor := obj.(*CallbackActorExecutor)
			executor.doTimeout()
		}
	})
	executor.Task = task
}

// 把一条 MessageRequest 转成后续 actor 真正执行时需要的上下文对象 wraper
func commonExecute(req *MessageRequest, msgSender *MsgSender, actor IUntypedActor) wraper {
	var sender ActorRef

	// srcHost := req.SrcHost
	// srcPort := req.SrcPort
	srcMethod := req.SrcMethod
	srcSession := req.Session

	if IsNoSender(req) {
		sender = NoSender
	} else {
		sender = &DefaultActorRef{
			// Host:    srcHost,
			// Port:    int(srcPort),
			Method:  srcMethod,
			Session: srcSession,
			Sender:  msgSender,
		}
	}

	bytes := req.Data

	createInputHandler, ok := actor.(ICreateInputHandler)
	var input proto.Message
	if ok {
		input = createInputHandler.CreateInputObj()
		proto.Unmarshal(bytes, input)
	}
	return wraper{
		sender: sender,
		msg:    input,
		actor:  actor,
	}
}

type wraper struct { // 处理消息所需元素
	sender ActorRef      // 消息发送者引用
	msg    proto.Message // 反序列化后的消息对象
	actor  IUntypedActor // 目标actor
}

func callbackActorExecute(pool *tunny.Pool, callbackWraperChan chan wraper) {
	for {
		wrapper := <-callbackWraperChan
		go pool.Process(func() {
			actor := wrapper.actor

			senderHandler, ok := actor.(ISenderHandler)
			if ok {
				senderHandler.SetSender(wrapper.sender)
			}
			receiveHandler, ok := actor.(IReceiveHandler)
			if ok {
				receiveHandler.OnReceive(context.Background(), wrapper.msg)
			}
		})
	}
}
