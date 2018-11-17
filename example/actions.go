package main

import (
	"fmt"
	"time"

	"github.com/go-apibox/api"
	"github.com/go-apibox/websocket"
	"github.com/go-apibox/websocket/subscribe"
)

// 示例：
// ws=new WebSocket("ws://192.168.1.120:8080/?api_action=Test")
// ws.send('{"a":"date"}')
// ws.send('{"a":"echo","d":"hello"}')
//
// ws=new WebSocket("ws://192.168.1.120:8080/?api_action=Subscribe")
// ws.send('{"a":"subscribe","d":{"source1":["now","i"]}}')
// ws.send('{"a":"watch","d":{"id":["1"]}}')
// ws.send('{"a":"interval","d":10}')

var testServer *websocket.Server
var subscribeServer *websocket.Server

func init() {
	// websocket服务器测试
	testServer = websocket.NewServer()
	testServer.HandleConnFunc(func(conn *websocket.Conn) {
		// 连通后即执行
		go func() {
			for i := 0; ; i++ {
				time.Sleep(time.Second)
				if err := conn.Write(fmt.Sprintf("i=%d", i)); err != nil {
					// 错误后立即退出
					return
				}
			}
		}()
	})
	testServer.HandleActionFunc("date", func(conn *websocket.Conn, packetDecoder *websocket.PacketDecoder) {
		packet := websocket.NewPacket("date")
		packet.SetData(map[string]string{"date": time.Now().Format("2006-01-02 15:04:05")})
		if err := conn.Send(packet); err != nil {
			fmt.Println(err.Error())
		}
	})
	testServer.HandleActionFunc("echo", func(conn *websocket.Conn, packetDecoder *websocket.PacketDecoder) {
		var data string
		packet := websocket.NewPacket("echo")
		if err := packetDecoder.ParseData(&data); err != nil {
			packet.SetError("ParseDataError", err.Error())
		} else {
			packet.SetData(data)
		}
		if err := conn.Send(packet); err != nil {
			fmt.Println(err.Error())
		}
	})

	// websocket订阅服务测试
	subscribeServer = websocket.NewServer()
	subscribeHandler := subscribe.NewSubscribeHandler("id")
	subscribeHandler.ShowDebug = true
	subscribeHandler.SetFieldsChangeHandler(func(s *subscribe.SubscribeHandler, oldFields map[string][]string, newFields map[string][]string) {
		fmt.Println(">> fields change")
	})
	subscribeHandler.SetObjectIdsChangeHandler(func(s *subscribe.SubscribeHandler, oldObjectIds []string, newObjectIds []string) {
		fmt.Println(">> object ids change")
	})
	subscribeHandler.SetIntervalChangeHandler(func(s *subscribe.SubscribeHandler, oldInterval uint32, newInterval uint32) {
		fmt.Println(">> interval change")
	})
	subscribeServer.Handle(subscribeHandler)

	// 循环更新数据
	i := 0
	go subscribeHandler.Loop(func(s *subscribe.SubscribeHandler) {
		i++
		subscribeHandler.UpdateObjectData(
			"1",       // 对象ID
			"source1", // 数据源
			map[string]interface{}{ // 数据
				"i": i,
				"now": map[string]interface{}{
					"str":  time.Now().Format("2006-01-02 15:04:05"),
					"unix": time.Now().Unix(),
				},
			},
		)
	})
}

func TestAction(c *api.Context) interface{} {
	return testServer.Serve(c)
}

func SubscribeAction(c *api.Context) interface{} {
	return subscribeServer.Serve(c)
}
