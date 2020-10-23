package main

import (
	"os"
	"strconv"
)

func main() {
	//为四个节点生成公私钥
	GenRsaKeys()
	NodeTable := make(map[string]string)
	for i := 0; i <= 99; i++ {
		n := "N"
		n += strconv.Itoa(i)
		addr := "127.0.0.1:"
		addr += strconv.Itoa(i + 8000)
		NodeTable[n] = addr
	}
	nodeID := os.Args[1]
	if nodeID == "client" {
		ClientSendMessageAndListen(NodeTable) //启动客户端程序
	} else if nodeID == "node" {
		PBFT(NodeTable)
	}
	select {}
}
