package server

import (
	"errors"
	"fmt"
	"im-server/commons/errs"
	"im-server/commons/gmicro/utils"
	"im-server/commons/tools"
	"im-server/services/commonservices"
	"im-server/services/connectmanager/server/codec"
	"im-server/services/connectmanager/server/imcontext"
	"im-server/services/connectmanager/server/imhttpmsghandlers"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"
)

// 主服务器
// 启动 HTTP 服务，监听 WebSocket 连接
type ImWebsocketServer struct {
	MessageListener ImListener
}

// 异步启动服务器
func (server *ImWebsocketServer) AsyncStart(port int) {
	var mux *http.ServeMux = commonservices.GetDefaultHttpServeMux()
	mux.HandleFunc("/im", server.ImWsServer) // WebSocket 连接入口
	mux.HandleFunc("/im/publish", imhttpmsghandlers.ImHttpPubHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"ok"}`)
	})
	go http.ListenAndServe(fmt.Sprintf(":%d", port), mux)
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4196,
	WriteBufferSize: 1124,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// 建立连接
func (server *ImWebsocketServer) ImWsServer(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil) //升级 HTTP 连接到 WebSocket
	if err != nil {
		fmt.Println("Error during connect upgrade:", err)
		return
	}

	// 从 HTTP 请求中提取 Referer 和客户端 IP 地址
	referer := strings.TrimSpace(r.Header.Get("Origin"))
	if referer == "" {
		referer = strings.TrimSpace(r.Header.Get("Referer"))
	}

	// 获取客户端 IP 地址，优先使用 X-Real-Ip 头部
	clientIp := conn.RemoteAddr().String()
	realIp := strings.TrimSpace(r.Header.Get("X-Real-Ip"))
	if realIp != "" {
		clientIp = realIp
	}

	// 创建 ImWebsocketChild 实例，管理该连接
	child := &ImWebsocketChild{
		stopChan:         make(chan bool, 1),
		wsConn:           conn,
		isActive:         true,
		messageListener:  server.MessageListener,
		latestActiveTime: time.Now().UnixMilli(),
	}

	// 用 utils.SafeGo 启动后台监听，不阻塞主请求线程
	utils.SafeGo(func() {
		child.startWsListener(referer, clientIp)
	})
}
func (server *ImWebsocketServer) Stop() {

}

// 单个连接管理，每个客户端连接对应一个 ImWebsocketChild 实例
// 维护 WebSocket 连接
// 管理连接生命周期
// 处理心跳检测
type ImWebsocketChild struct {
	stopChan         chan bool
	wsConn           *websocket.Conn
	isActive         bool
	messageListener  ImListener
	latestActiveTime int64
	ticker           *time.Ticker
}

// 启动 WebSocket 监听器，处理客户端消息
func (child *ImWebsocketChild) startWsListener(referer, clientIp string) {
	handler := IMWebsocketMsgHandler{child.messageListener}
	ctx := &WsHandleContextImpl{
		conn:       child.wsConn,
		wsChild:    child,
		lock:       &sync.RWMutex{},
		attachment: &sync.Map{},
	}
	imcontext.SetContextAttr(ctx, imcontext.StateKey_ConnectSession, tools.GenerateUUIDShort11())
	imcontext.SetContextAttr(ctx, imcontext.StateKey_ConnectCreateTime, time.Now().UnixMilli())
	imcontext.SetContextAttr(ctx, imcontext.StateKey_CtxLocker, &sync.Mutex{})
	imcontext.SetContextAttr(ctx, imcontext.StateKey_Limiter, rate.NewLimiter(100, 10))
	imcontext.SetContextAttr(ctx, imcontext.StateKey_Referer, referer)
	imcontext.SetContextAttr(ctx, imcontext.StateKey_ClientIp, clientIp)

	//start ticker
	child.startTicker(ctx, handler)

	for child.isActive {
		_, message, err := child.wsConn.ReadMessage() // 读取客户端消息，阻塞直到有消息或连接关闭
		//record
		child.latestActiveTime = time.Now().UnixMilli()

		if err != nil {
			if child.isActive {
				child.Stop()
				handler.HandleException(ctx, errs.IMErrorCode_CONNECT_CLOSE_NET_ERR, err)
			}
			break
		}

		//decode
		wsMsg := &codec.ImWebsocketMsg{}
		err = tools.PbUnMarshal(message, wsMsg) // protobuf 反序列化
		if err != nil {
			fmt.Println("failed to decode pb data:", err)
			child.Stop()
			handler.HandleException(ctx, errs.IMErrorCode_CONNECT_CLOSE_PB_DECODE_FAIL, err)
			break
		}

		//decrypt
		wsMsg.Decrypt(ctx) // 解密

		handler.HandleRead(ctx, wsMsg) // 送入消息分发
	}
}

func (child *ImWebsocketChild) startTicker(ctx imcontext.WsHandleContext, handler IMWebsocketMsgHandler) {
	if child.ticker == nil {
		child.ticker = time.NewTicker(5 * time.Second)
	} else {
		child.ticker.Reset(5 * time.Second)
	}
	go func(ticker *time.Ticker) {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				current := time.Now().UnixMilli()
				interval := current - child.latestActiveTime
				if interval > 300*1000 {
					child.Stop()
					handler.HandleException(ctx, errs.IMErrorCode_CONNECT_CLOSE_HEARTBEAT_TIMEOUT, errors.New("user inactive more than 5min"))
					return
				}
			case <-child.stopChan:
				return
			}
		}
	}(child.ticker)
}

func (child *ImWebsocketChild) Stop() {
	if child.isActive {
		child.isActive = false
		child.stopChan <- true
		if child.wsConn != nil {
			child.wsConn.Close()
		}
		close(child.stopChan)
	}
}

type WsHandleContextImpl struct {
	wsChild    *ImWebsocketChild
	conn       *websocket.Conn
	attachment interface{}
	lock       *sync.RWMutex
}

// 将一条消息发送到客户端
func (ctx *WsHandleContextImpl) Write(message interface{}) {
	imMsg, ok := message.(codec.IMessage)
	if ok {
		wsImMsg := imMsg.ToImWebsocketMsg()
		//encrypt
		wsImMsg.Encrypt(ctx)
		bs, err := tools.PbMarshal(wsImMsg)
		if err == nil {
			ctx.lock.Lock()
			defer ctx.lock.Unlock()
			_ = ctx.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			err = ctx.conn.WriteMessage(websocket.BinaryMessage, bs)
			if err != nil {
				fmt.Println("write result:", err)
			}
		} else {
			fmt.Println(err)
		}
	} else {
		fmt.Println("No IMessage to transfer to WebsocketMsg.")
	}
}

func (ctx *WsHandleContextImpl) Close(err error) {
	if ctx.wsChild != nil {
		ctx.wsChild.Stop()
	}
}
func (ctx *WsHandleContextImpl) Attachment() imcontext.Attachment {
	return ctx.attachment
}
func (ctx *WsHandleContextImpl) SetAttachment(attachment imcontext.Attachment) {
	ctx.attachment = attachment
}
func (ctx *WsHandleContextImpl) IsActive() bool {
	return ctx.wsChild.isActive
}
func (ctx *WsHandleContextImpl) RemoteAddr() string {
	if ctx.conn != nil {
		return ctx.conn.RemoteAddr().String()
	} else {
		return ""
	}
}
