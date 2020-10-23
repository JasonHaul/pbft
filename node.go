package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/astaxie/beego/logs"
)

type Node struct {
	nodeID          string //节点ID
	pnodeID         string //主节点ID
	lock            sync.Mutex
	CurCount        int //当前处理消息数量
	CurSequence     int //当前消息号，只针对主节点有效
	minSequence     int
	maxSequence     int
	addr            string              //节点监听地址
	bPrimary        bool                //是否主节点
	rsaPrivKey      []byte              //RSA私钥
	rsaPubKey       []byte              //RSA公钥
	NodeTable       map[string]string   // key=nodeID, value=url
	NodeMsgEntrance map[int]*NodeChanel //节点每个线程对应的消息通道，接收对应消息
}

var plog = logs.NewLogger()

func PBFT(nodeTable map[string]string) {
	plog.SetLogger(logs.AdapterFile, `{"filename":"pbft.log"}`)
	plog.Async()
	n0 := NewNode(true, "N0", "127.0.0.1:8000", nodeTable)
	go n0.TcpListen()
	for i := 1; i <= 99; i++ {
		n := NewNode(false, "N"+strconv.Itoa(i), nodeTable["N"+strconv.Itoa(i)], nodeTable)
		go n.TcpListen()
	}
}

func NewNode(primary bool, nodeID, addr string, nodeTable map[string]string) *Node {
	node := new(Node)
	node.nodeID = nodeID
	node.addr = addr
	node.NodeTable = nodeTable
	node.bPrimary = primary
	node.rsaPrivKey = getPivKey(nodeID)
	node.rsaPubKey = getPubKey(nodeID)
	node.NodeMsgEntrance = make(map[int]*NodeChanel)
	return node
}

func (n *Node) TcpListen() {
	listen, err := net.Listen("tcp", n.addr)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("节点开启监听，地址：%s\n", n.addr)
	plog.Info("节点开启监听，地址：%s", n.addr)
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
		//处理消息
		n.dispatchMsg(b)
	}

}

//将消息分发至节点各个线程,或者做相应处理
//对于每个线程设置超时时间
//对于超过多少个消息，进行舍弃（主节点）
//TODO:对于上下限距离过大的，对于下限线程进行关闭
func (n *Node) dispatchMsg(data []byte) {
	cmd, content := splitMessage(data)
	switch command(cmd) {
	case cRequest:
		n.handleRequest(content)
	case cPrePrepare:
		n.handlePrePrepare(content)
	case cPrepare:
		n.handlePrepare(data)
	case cCommit:
		n.handleCommit(data)
	}
	n.handleDisableNode()
}

func (n *Node) handleRequest(content []byte) {
	if !n.bPrimary {
		return
	}
	if n.CurCount > 100 {
		return
	}
	//使用json解析出Request结构体
	r := new(Request)
	err := json.Unmarshal(content, r)
	if err != nil {
		log.Panic(err)
	}

	//获取消息摘要
	digest := getDigest(*r)

	//主节点对消息摘要进行签名
	n.CurSequence++
	digestByte, _ := hex.DecodeString(digest)
	signInfo := n.RsaSignWithSha256(digestByte, n.rsaPrivKey)
	//拼接成PrePrepare，准备发往follower节点
	pp := PrePrepare{*r, digest, n.CurSequence, signInfo}
	b, err := json.Marshal(pp)
	if err != nil {
		log.Panic(err)
	}
	//进行PrePrepare广播
	message := jointMessage(cPrePrepare, b)
	func() {
		for i := range n.NodeTable {
			if i == n.nodeID {
				continue
			}
			tcpDial(message, n.NodeTable[i])
		}
	}()

	n.dispatchMsg(message)
}

func (n *Node) handlePrePrepare(content []byte) {
	//使用json解析出PrePrepare结构体
	pp := new(PrePrepare)
	err := json.Unmarshal(content, pp)
	if err != nil {
		log.Panic(err)
	}
	plog.Info("%s收到主节点PP消息%d", n.nodeID, pp.SequenceID)

	n.CurSequence = pp.SequenceID
	nodeChanel := NewNodeChanel(n)
	n.lock.Lock()
	n.CurCount++
	n.NodeMsgEntrance[pp.SequenceID] = nodeChanel
	n.lock.Unlock()

	go nodeChanel.msgProcessing(pp)
}

func (n *Node) handlePrepare(data []byte) {
	//使用json解析出Prepare结构体
	_, content := splitMessage(data)
	pre := new(Prepare)
	err := json.Unmarshal(content, pre)
	if err != nil {
		log.Panic(err)
	}
	plog.Info("%s收到%sPre消息%d", n.nodeID, pre.NodeID, pre.SequenceID)

	go func() {
		for i := 0; i <= 10; i++ {
			n.lock.Lock()
			nodechan, ok := n.NodeMsgEntrance[pre.SequenceID]
			n.lock.Unlock()
			if ok {
				nodechan.MsgEntrance <- data
				return
			}
			time.Sleep(time.Second * 1)
		}
	}()
}

func (n *Node) handleCommit(data []byte) {
	//使用json解析出Commit结构体
	_, content := splitMessage(data)
	c := new(Commit)
	err := json.Unmarshal(content, c)
	if err != nil {
		log.Panic(err)
	}
	plog.Info("%s收到%sC消息%d", n.nodeID, c.NodeID, c.SequenceID)

	go func() {
		for i := 0; i <= 10; i++ {
			n.lock.Lock()
			nodechan, ok := n.NodeMsgEntrance[c.SequenceID]
			n.lock.Unlock()
			if ok {
				nodechan.MsgEntrance <- data
				return
			}
			time.Sleep(time.Second * 1)
		}
	}()
}

func (n *Node) handleDisableNode() {

}

func (n *Node) broadcast(cmd command, content []byte) {
	message := jointMessage(cmd, content)
	for i := range n.NodeTable {
		tcpDial(message, n.NodeTable[i])
	}
}
