/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file (Revised 2020)
 *
 * MP2 Starter template version
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "common.h"
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "Message.h"
#include "Queue.h"


#define NUM_KEY_REPLICAS 3
#define QUORUM_COUNT 2
#define TIMEOUT 10

/**
 * CLASS NAME: TransInfo
 *
 * DESCRIPTION: This class encapsulates all the important information for a transaction 
 */
class TransInfo {
public:
    int transID;
    //Address coordAddr;
    int creationTimestamp;
    MessageType transType;
	string key;
	string value;
	int successRepCnt;
    int totalRepCnt; 
    bool dropped; 
    TransInfo(int transID, int creationTimestamp, MessageType type, string key, string value);
};

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
    // order:  predecessors[1], predecessors[0], myself, successors[0], successors[1] 
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> successors; //2 successors
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> predecessors; //2 predecessors 
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
	// string delimiter
	string delimiter;
	// all the transacitions i'm coordinating / coordinated. 
	map<int, TransInfo> transMap; 


public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	bool compareNode(const Node& first, const Node& second) {
		return first.nodeHashCode < second.nodeHashCode;
	}

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();
	
	~MP2Node();


    /**** my added methods ***/ 
    void checkTimeOutTransactions();
    void establishNeighbors(string key, ReplicaType replicaType);
    void clientCRUD(string key, string value, MessageType type);
    void onCRUDMessage(Message& msg);
    void onReplyMessage(Message& msg);
    //void onReadReplyMessage(Message& msg);

    void logSuccess(MessageType type, int transID, string key, string value, bool isCoordinator);
    void logFailure(MessageType type, int transID, string key, string value, bool isCoordinator);
};













#endif /* MP2NODE_H_ */
