package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"net"
	"strings"
	"time"
)

var SendCount int
var GetCount int
var NodeTable map[string]string

//客户端的监听地址
var clientAddr = "127.0.0.1:8888"

func ClientSendMessageAndListen(nodeTable map[string]string) {
	ClientMsgMap = make(map[int]*ClientMsg)
	NodeTable = nodeTable
	//开启客户端的本地监听（主要用来接收节点的reply信息）
	go ClientTcpListen()
	h, _, _ := formatTimeHeader(time.Now())
	fmt.Printf("%s: 客户端开启监听，地址：%s\n", h, clientAddr)

	//stdReader := bufio.NewReader(os.Stdin)
	for i := 0; i < 100; i++ {
		data := "hello world!"
		// data, err := stdReader.ReadString('\n')
		// if err != nil {
		// 	fmt.Println("Error reading from stdin")
		// 	panic(err)
		// }
		r := new(Request)
		r.Timestamp = time.Now().UnixNano()
		r.ClientAddr = clientAddr
		r.Message.ID = getRandom()
		//消息内容就是用户的输入
		r.Message.Content = strings.TrimSpace(data)
		br, err := json.Marshal(r)
		if err != nil {
			log.Panic(err)
		}
		fmt.Println(string(br))
		content := jointMessage(cRequest, br)
		//默认N0为主节点，直接把请求信息发送至N0
		tcpDial(content, NodeTable["N0"])
		SendCount++
	}

	time.Sleep(time.Second * 100)

	fmt.Printf("发送连接数：%d\n", SendCount)
	fmt.Printf("接受连接数：%d\n", GetCount)
}

//返回一个十位数的随机数，作为msgid
func getRandom() int {
	x := big.NewInt(10000000000)
	for {
		result, err := rand.Int(rand.Reader, x)
		if err != nil {
			log.Panic(err)
		}
		if result.Int64() > 1000000000 {
			return int(result.Int64())
		}
	}
}

func handleReply(data []byte) {
	cmd, content := splitMessage(data)
	if cmd != "reply" {
		return
	}
	r := new(Reply)
	err := json.Unmarshal(content, r)
	if err != nil {
		log.Panic(err)
	}
	h, _, _ := formatTimeHeader(time.Now())
	fmt.Printf("%s: 收到%s节点消息%d\n", h, r.NodeID, r.Message.ID)
	GetCount++

}

//客户端使用的tcp监听
func ClientTcpListen() {
	listen, err := net.Listen("tcp", clientAddr)
	if err != nil {
		log.Panic(err)
	}
	defer listen.Close()

	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Panic(err)
		}
		b, err := ioutil.ReadAll(conn)
		if err != nil {
			log.Panic(err)
		}
		handleReply(b)
	}

}
