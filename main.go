package main

import (
	"net"
	"fmt"
	"flag"
	"log"
	"encoding/json"
	"github.com/certik-project/repository"
	"github.com/certik-project/utils"
)

//Message sent to a server to sync connection status of the sender node
type NodeMessage struct {
	IpAddress 		string				`json:"ipAddress"`
	Port      		int					`json:"port"`
	IsAttempting	bool				`json:"isAttempting"`
	IsConnected 	bool				`json:"isConnected"`
	Peers			[]repository.Peer  	`json:"peers"`
}

//Initialize a new node message
func NewNodeMessage(
	ipAddress string,
	port int,
	isAttempting bool,
	isConnected bool) NodeMessage {
	return NodeMessage {
		IpAddress: ipAddress,
		Port: port,
		IsAttempting: isAttempting,
		IsConnected: isConnected,
	}
}

var (
	clusterIp = flag.String("clusterIp", "", "Active node ip to connect to the cluster")
	clusterPort = flag.Int("clusterPort", 0, "Active Port to connect to the cluster")
	nodeIp = flag.String("nodeIp", "0.0.0.0", "The ip address of this node")
	nodePort = flag.Int("nodePort", 8080, "Port for this node to listen on")

	sqlRepository = repository.NewSqlRepository()
)

func main() {
	flag.Parse()
	if *clusterIp != "" {
		sendConnectionRequest()
		broadcastNodeInfo()
	} else {
		log.Println("Creating master node on port", *nodePort)
	}

	listen()
}

// Send the request for this node to join the active cluster
func sendConnectionRequest() {
	clusterInfo := fmt.Sprintf("%v:%v", *clusterIp, *clusterPort)
	log.Println("Connecting to cluster via node", clusterInfo)

	conn, _ := net.Dial("tcp", clusterInfo)
	defer conn.Close()
	for {
		// send request to the cluster node
		connectionRequest := NewNodeMessage(*nodeIp, *nodePort, true, false)
		encoder := json.NewEncoder(conn)
		encoder.Encode(connectionRequest)

		// listen for reply from the cluster node
		var nodeMessage NodeMessage
		decoder := json.NewDecoder(conn)
		err := decoder.Decode(&nodeMessage)
		utils.CheckForError(err)
		log.Println("Cluster responded with:", nodeMessage)

		// Save the peers from the response
		savePeers(nodeMessage.Peers)
		return
	}
}

// Listen for incoming messages
func listen() {
	nodePortSuffix := fmt.Sprintf(":%v", *nodePort)
	listener, err := net.Listen("tcp", nodePortSuffix)
	utils.CheckForError(err)
	log.Println("Listening for incoming messages...")
	for {
		conn, err := listener.Accept()
		if err != nil {
			// handle error
		}
		go handleIncomingMessage(conn)
	}
}

// Handle a new incoming message to this node
func handleIncomingMessage(conn net.Conn) {
	defer conn.Close()
	for {
		var nodeMessage NodeMessage
		decoder := json.NewDecoder(conn)
		err := decoder.Decode(&nodeMessage)

		// successfully marshaled a nodeMessage
		if err == nil {
			nodeMessage = handleNodeMessage(nodeMessage)
			encoder := json.NewEncoder(conn)
			encoder.Encode(nodeMessage)
			return
		}
	}
}

// Handle a node message sent to this server
func handleNodeMessage(nodeMessage NodeMessage) NodeMessage {
	switch {
	case nodeMessage.IsAttempting:
		return acceptConnectionRequest(nodeMessage)
	case nodeMessage.IsConnected:
		return syncNewPeer(nodeMessage)
	}

	return nodeMessage

}

// Accept the connection request to the cluster from the new node
func acceptConnectionRequest(nodeMessage NodeMessage) NodeMessage {
	log.Printf("Accepting Connection from %v:%v...", nodeMessage.IpAddress, nodeMessage.Port)
	// get list of peers to send back to node
	nodeMessage.Peers = GetPeers()

	// store to new peer connection to the db
	syncNewPeer(nodeMessage)

	// Add this node as a possible peer
	nodeMessage.Peers = append(nodeMessage.Peers, repository.Peer {
		IpAddress: *nodeIp,
		Port: *nodePort,
	})
	// the new node is now connected, send back a success response to the new node
	nodeMessage.IsConnected = true
	return nodeMessage
}

// Add the new peer to the list of peers
func syncNewPeer(nodeMessage NodeMessage) NodeMessage {
	log.Printf("Received new peer %v:%v", nodeMessage.IpAddress, nodeMessage.Port)
	newPeer := repository.Peer {
		IpAddress: nodeMessage.IpAddress,
		Port: nodeMessage.Port,
	}
	savePeer(newPeer)
	return nodeMessage
}

// broadcast the initialization of this node to each peer
func broadcastNodeInfo() {
	peers := GetPeers()
	log.Println("Broadcasting this node's info to peers...")
	for _, peer := range peers {
		address := fmt.Sprintf("%v:%v", peer.IpAddress, peer.Port)
		conn, _ := net.Dial("tcp", address)
		encoder := json.NewEncoder(conn)
		nodeMessage := NewNodeMessage(*nodeIp, *nodePort, false, true)
		encoder.Encode(nodeMessage)
		conn.Close()
	}
}

// Repository Methods

// Get all peers from db
func GetPeers() []repository.Peer {
	nodeKey := fmt.Sprintf("%v:%v", *nodeIp, *nodePort)
	return sqlRepository.GetPeers(nodeKey)
}

// Save a given list of peers
func savePeers(peers []repository.Peer) {
	for _, peer := range peers {
		savePeer(peer)
	}
}

// Save a single peer
func savePeer(peer repository.Peer) {
	if peer.IpAddress == *nodeIp && peer.Port == *nodePort {
		return
	}
	nodeKey := fmt.Sprintf("%v:%v", *nodeIp, *nodePort)
	sqlRepository.SavePeer(nodeKey, peer.IpAddress, peer.Port)
}