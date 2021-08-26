/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition (Revised 2020)
 *
 * MP2 Starter template version
 **********************************/
#include "MP2Node.h"
#include "common.h"
#include <utility>

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
    // 这里为什么没用this 
	ht = new HashTable();
	this->memberNode->addr = *address;
	this->delimiter = "::";
}

TransInfo::TransInfo(int transID, int creationTimestamp, MessageType type, string key, string value){
    this-> transID = transID;
    //this-> coordAddr = coordAddr;
    this-> creationTimestamp = creationTimestamp;
    this-> transType = type; 
    this-> key = key;
    this-> value = value; 
    this-> successRepCnt = 0; 
    this-> totalRepCnt = 0; 
    this-> dropped = false; 
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
}

/**
* FUNCTION NAME: updateRing
*
* DESCRIPTION: This function does the following:
*                 1) Gets the current membership list from the Membership Protocol (MP1Node)
*                    The membership list is returned as a vector of Nodes. See Node class in Node.h
*                 2) Constructs the ring based on the membership list
*                 3) Calls the Stabilization Protocol
*/
void MP2Node::updateRing() {
	/*
     * Implement this. Parts of it are already implemented
     */
    vector<Node> curMemList;
    bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
    // a vector of Node 
	curMemList = getMembershipList();
    //cerr<< "received ring membership list"<<curMemList.size()<<endl;
	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

    
    if (ring.size() != curMemList.size()){
        change = true;
    } else {
        for(size_t i=0; i < curMemList.size(); ++i)
        {
            if (curMemList[i].getHashCode() != ring[i].getHashCode())
            {
                change = true;
                break;
            }
        }
    }

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
    if (change){
        //cerr<< ring.size() << ' '<<curMemList.size()<<endl;
        ring = curMemList;
        if (!ht->isEmpty()){
            stabilizationProtocol();
        }
    }

    /* step 4 added by me: identify timeout transactions. 
    */
    
    checkTimeOutTransactions();
    
    
}
void MP2Node::checkTimeOutTransactions(){
    //cerr<< "checking timeout transactions"<<endl;
    for (auto& it: transMap){
        TransInfo& curTrans = it.second;
        if (!curTrans.dropped){
            //cerr << "transaction "<<curTrans.transID<<"has waited for"<<par->getcurrtime() - curTrans.creationTimestamp<<endl;
            if (par->getcurrtime() - curTrans.creationTimestamp >= TIMEOUT){
                logFailure(curTrans.transType, curTrans.transID, curTrans.key, curTrans.value, true);
                curTrans.dropped = true; 
            }
        }
    }
}


/**
* FUNCTION NAME: getMembershipList
*
* DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
*                 i) generates the hash code for each member
*                 ii) populates the ring member in MP2Node class
*                 It returns a vector of Nodes. Each element in the vector contain the following fields:
*                 a) Address of the node
*                 b) Hash code obtained by consistent hashing of the Address
*/
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
        // #emplace_back : Appends a new element to the end of the container.
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
* FUNCTION NAME: hashFunction
*
* DESCRIPTION: This functions hashes the key and returns the position on the ring
*                 HASH FUNCTION USED FOR CONSISTENT HASHING
*
* RETURNS:
* size_t position on the ring
*/
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

void MP2Node::clientCRUD(string key, string value, MessageType type){
    //cerr<<"sending client messsage "<< type << "with key "<< key <<" value " << value << endl;
    vector<Node> replicas = findNodes(key);
    g_transID++;
    assert(replicas.size() >= NUM_KEY_REPLICAS);
    TransInfo transInfo(g_transID,  par->getcurrtime(), type, key, value);
    transMap.insert({g_transID, transInfo});
    //a dummy initalized message
    Message msg(g_transID,memberNode->addr, CREATE,"",""); 
    // route message to all replicas  
    for (int i=0;i<NUM_KEY_REPLICAS;++i){
        switch(type){
            case CREATE: 
                msg = Message(g_transID, memberNode->addr, CREATE, key, value, static_cast<ReplicaType>(i)); break;
            case READ: 
                msg = Message(g_transID, memberNode->addr, READ, key); break;
            case UPDATE: 
                msg = Message(g_transID, memberNode->addr, UPDATE, key, value, static_cast<ReplicaType>(i)); break;
            case DELETE: 
                msg = Message(g_transID, memberNode->addr, DELETE, key); break;
            default:
                cerr<< "ERROR: expecting CRUD message type."<<endl;
                exit(999);
        }
	    emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
    }
}

/**
* FUNCTION NAME: clientCreate
*
* DESCRIPTION: client side CREATE API
*                 The function does the following:
*                 1) Constructs the message
*                 2) Finds the replicas of this key
*                 3) Sends a message to the replica (maybe should send to all replicase? )
*/
void MP2Node::clientCreate(string key, string value) {
    // client wants to create a key-value entry (a write request)
    // the coordinator find the replicas of the key 
    // route request to replicas, wait for a quorum of replicas to acknowledge the completition of writes
	/*
	* Implement this
	*/
    clientCRUD(key,value,CREATE);
}

/**
* FUNCTION NAME: clientRead
*
* DESCRIPTION: client side READ API
*                 The function does the following:
*                 1) Constructs the message
*                 2) Finds the replicas of this key
*                 3) Sends a message to the replica
*/
void MP2Node::clientRead(string key){
	/*
	* Implement this
	*/
    clientCRUD(key,"",READ);
}

/**
* FUNCTION NAME: clientUpdate
*
* DESCRIPTION: client side UPDATE API
*                 The function does the following:
*                 1) Constructs the message
*                 2) Finds the replicas of this key
*                 3) Sends a message to the replica
*/
void MP2Node::clientUpdate(string key, string value){
	/*
    * Implement this
    */
    clientCRUD(key,value,UPDATE);
}

/**
* FUNCTION NAME: clientDelete
*
* DESCRIPTION: client side DELETE API
*                 The function does the following:
*                 1) Constructs the message
*                 2) Finds the replicas of this key
*                 3) Sends a message to the replica
*/
void MP2Node::clientDelete(string key){
	/*
	* Implement this
	*/
    clientCRUD(key,"",DELETE);
}

/**
* FUNCTION NAME: createKeyValue
*
* DESCRIPTION: Server side CREATE API
*                    The function does the following:
*                    1) Inserts key value into the local hash table
*                    2) Return true or false based on success or failure
*/
bool MP2Node::createKeyValue(string key, string value, ReplicaType replicaType) {
	/*
	 * Implement this
	 */
	// hashTable use emplace to insert into map, may fail if key already exist 
    // therefore, try update first
    //cerr<< "creating key-value at" << memberNode->addr.addr << " replica type : "<< replica<< endl;
    if (!ht->read(key).empty()) {
		// Key already in hashtable 
        return updateKeyValue(key,value,replicaType);
	}

    establishNeighbors(key, replicaType);
    
    string ent = Entry(value, par->getcurrtime(), replicaType).convertToString();
    return ht->create(key, ent);
}

void MP2Node::establishNeighbors(string key, ReplicaType replicaType){
    auto replicas = findNodes(key);
    //assert(replicas[replicaType].nodeAddress == memberNode->addr);
    switch (replicaType){
        case PRIMARY: successors = {replicas[1], replicas[2]};break;

        case SECONDARY: 
            if (successors.size() >= 1){
                successors[0] = replicas[2];
            } else { 
                successors = {replicas[2]};
            };

            if (predecessors.size() >= 1){
                predecessors[0] = replicas[0];
            } else { 
                predecessors = {replicas[0]};
            };

            break;

        case TERTIARY: predecessors = {replicas[1], replicas[0]}; break;
        default:
            cerr<< "unknown replica type"<<endl;
            exit(999);
    }
}

/**
* FUNCTION NAME: readKey
*
* DESCRIPTION: Server side READ API
*                 This function does the following:
*                 1) Read key from local hash table
*                 2) Return value
*/
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
    string value = ht->read(key);
    if (!value.empty()){value = Entry(value).value;}
    return value;
}

/**
* FUNCTION NAME: updateKeyValue
*
* DESCRIPTION: Server side UPDATE API
*                 This function does the following:
*                 1) Update the key to the new value in the local hash table
*                 2) Return true or false based on success or failure
*/
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
    establishNeighbors(key, replica);
    Entry ent(value, par->getcurrtime(), replica);
    return ht -> update(key, ent.convertToString());
}

/**
* FUNCTION NAME: deleteKey
*
* DESCRIPTION: Server side DELETE API
*                 This function does the following:
*                 1) Delete the key from the local hash table
*                 2) Return true or false based on success or failure
*/
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
    return ht -> deleteKey(key);
}

/**
* FUNCTION NAME: checkMessages
*
* DESCRIPTION: This function is the message handler of this node.
*                 This function does the following:
*                 1) Pops messages from the queue
*                 2) Handles the messages according to message types
*/
void MP2Node::checkMessages() {
	/*
	* Implement this. Parts of it are already implemented
	*/
	char * data;
	int size;

	/*
	* Declare your local variables here
	*/

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
        Message msg(message);
        /*
		 * Handle the message types here
		 */
        // enum MessageType {CREATE, READ, UPDATE, DELETE, REPLY, READREPLY};
        // reply: replica reply coordinator with status 
        // readreply: reply from read (receive a string)

        /*
        * This function should also ensure all READ and UPDATE operation
        * get QUORUM replies
        */
        switch(msg.type){
            // i'm replica, receive command to create key-value
            case CREATE: onCRUDMessage(msg); break;
            // i'm replica, receive command to read key 
            case READ: onCRUDMessage(msg); break;
            // i'm replica, receive command to update key-value
            case UPDATE: onCRUDMessage(msg); break;
            // i'm replica, receive command to delete key
            case DELETE: onCRUDMessage(msg); break;
            // I'm coordinator, receive reply from replicas, need to keep track if transactions receive quorum
            case REPLY: onReplyMessage(msg); break; 
            // i'm coordinator, receive read reply from replicas, need to keep track if transactions receive quorum
            case READREPLY: onReplyMessage(msg); break;
            default:
                cerr<< "ERROR: unknown message type."<<msg.type<<endl;
                exit(999);
        }

		
	}

	
}
void MP2Node::onCRUDMessage(Message& msg){
    //cerr<<" received "<< msg.type<< " message"<<endl;
    bool isCoordinator = false; 
    bool success = false;
    string value = ""; 
    switch(msg.type){
        case CREATE: success = createKeyValue(msg.key, msg.value, msg.replica); break;
        case READ: value = readKey(msg.key); break;
        case UPDATE: success = updateKeyValue(msg.key, msg.value, msg.replica); break;
        case DELETE: success = deletekey(msg.key); break;
        default:
            cerr<< "ERROR: expecting CRUD message type."<<endl;
            exit(999);
    }
    if (!value.empty()){// Read success message
        logSuccess(msg.type, msg.transID, msg.key, value, isCoordinator);  
    } else if (success){ // CUD success message
        logSuccess(msg.type, msg.transID, msg.key, msg.value, isCoordinator); 
	} else {
        logFailure(msg.type, msg.transID, msg.key, msg.value, isCoordinator);
	}
    // notify coordinator
    // construct a reply message
    Message repMsg(msg.transID, memberNode->addr, REPLY, success);
    if (msg.type == READ){ // construct a read reply message
        cerr<< "constructing a READREPLY message with value: "<< value<<endl;
        repMsg = Message(msg.transID, memberNode->addr, value);
    } 
	emulNet->ENsend(&memberNode->addr, &msg.fromAddr, repMsg.toString());
    //cerr<< "sent reply message"<<endl;
	

}


/* 
increment reply received count for that particular transaction
once reach quorum or all message received still not at quorum 
log success/ failure 
*/ 
void MP2Node::onReplyMessage(Message& msg){
    // reply message doesnt carry key, only readreply message carry value 
    //cerr<<"received reply message from transaction: "<< msg.transID << endl;
    bool isCoordinator = true; 
    if (transMap.find(msg.transID) == transMap.end()){ // this shouldn't happen
        cerr<<"don't have "<< msg.transID << "in transMap!"<<endl;
        exit(999);
    } 
    TransInfo& curTrans = transMap.at(msg.transID); // throws exception if missing key 
    curTrans.totalRepCnt++; 
    /*
    if (msg.type == READREPLY){
        cerr<< "coordinator received READREPLY for transaction: "<< msg.transID << " value in the message:  "<<msg.value<<endl;
        cerr<< "is val empty? "<< msg.value.empty() << " is msg.success? "<< msg.success<<endl;
    }
    */
    bool readReplySuccess  = msg.type == READREPLY  && !msg.value.empty(); 
    bool replySuccess = msg.type == REPLY && msg.success;
    
    if (readReplySuccess || replySuccess){
        //cerr<< "increment success count for transaction: "<< msg.transID<<endl;
        curTrans.successRepCnt++;
    }  

    if (curTrans.dropped){return;} 

    // log messages for alive transactions. 
    //cerr<< "transaction : "<< msg.transID << " success count : "<<curTrans.successRepCnt << endl;
    string value = (msg.type == READREPLY)? msg.value : curTrans.value;
    if (curTrans.successRepCnt == QUORUM_COUNT){ 
        //cerr<< "transaction: "<< msg.transID << "reached quorum"<<endl;
        //cerr << "logging successeful "<<transMap[msg.transID].type << endl;
        logSuccess(curTrans.transType, curTrans.transID, curTrans.key, value, isCoordinator);
        curTrans.dropped = true; // make sure each transaction report success only once. 
        return;
    }
    if (curTrans.totalRepCnt == NUM_KEY_REPLICAS){
        logFailure(curTrans.transType, curTrans.transID, curTrans.key, value, isCoordinator);
        curTrans.dropped = true;
        return ;
    }


}


void MP2Node::logSuccess(MessageType type, int transID, string key, string value, bool isCoordinator){
    
    //cerr<< "is Coordinator? "<< isCoordinator<< " logging success for transaction: "<< transID <<"key: "<< key<< " value: "<<value<<" message type: "<< type << endl;

    switch(type){
        case CREATE: log->logCreateSuccess(&memberNode->addr, isCoordinator, transID, key, value); break;
        case READ: 
            log->logReadSuccess(&memberNode->addr, isCoordinator, transID, key, value); 
            //cerr<< "read value: "<<value<<endl;
            break;
        case UPDATE: log->logUpdateSuccess(&memberNode->addr, isCoordinator, transID, key, value); break;
        case DELETE: log->logDeleteSuccess(&memberNode->addr, isCoordinator, transID, key); break;
        default:
            cerr<< "ERROR: unknown message type: "<<type<<endl;
            exit(999);
    }
}

void MP2Node::logFailure(MessageType type, int transID, string key, string value, bool isCoordinator){
    //cerr<< "is Coordinator? "<< isCoordinator<< " logging failure for transaction: "<< transID <<"key: "<< key<< " value: "<<value<<" message type: "<< type << endl;
    switch(type){
        case CREATE: log->logCreateFail(&memberNode->addr, isCoordinator, transID, key, value); break;
        case READ: log->logReadFail(&memberNode->addr, isCoordinator, transID, key); break;
        case UPDATE: log->logUpdateFail(&memberNode->addr, isCoordinator, transID, key, value); break;
        case DELETE: log->logDeleteFail(&memberNode->addr, isCoordinator, transID, key); break;
        default:
            cerr<< "ERROR: unknown message type: "<<type<<endl;
            exit(999);
    }
    
}

/**
* FUNCTION NAME: findNodes
*
* DESCRIPTION: Find the replicas of the given keyfunction
*                 This function is responsible for finding the replicas of a key
*/
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		} else {
			// go through the ring until pos <= node
			for (int i=1; i < (int)ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
	if ( memberNode->bFailed ) {
		return false;
	}
	else {
		return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
	}
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
* FUNCTION NAME: stabilizationProtocol
*
* DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
*                 It ensures that there always 3 copies of all keys in the DHT at all times
*                 The function does the following:
*                1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
*                Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
*/
void MP2Node::stabilizationProtocol() {
	/*
	* Implement this
    */

    //cerr<< "starts stabilization protocol" << endl;
    bool repChanged[3]; // see if any of the 3 replicas changed.
    for (auto& it : ht-> retPrimaryPairs()){
        string key = it.first;
        string value = it.second;
        vector<Node> replicas = findNodes(key);
        repChanged[0] =  replicas[0].nodeAddress != memberNode->addr;

        if (successors.size() >= 1){
            repChanged[1] = successors[0].nodeAddress != replicas[1].nodeAddress;
        } else { repChanged[1] = true; }

        if (successors.size() >= 2){
            repChanged[2] = successors[1].nodeAddress != replicas[2].nodeAddress;
        } else { repChanged[2] = true; }

        if (repChanged[0] || repChanged[1] || repChanged[2]){
            ht->deleteKey(key);
            clientCreate(key, value);
        }
    }

    for (auto& it : ht-> retSecondaryPairs()){
        string key = it.first;
        string value = it.second;
        vector<Node> replicas = findNodes(key);

        if (predecessors.size()>=1){
            repChanged[0] = predecessors[0].nodeAddress != replicas[0].nodeAddress;
        } else { repChanged[0] = true; }
        
        repChanged[1] =  replicas[1].nodeAddress != memberNode->addr;

        if (successors.size() >= 1){
            repChanged[2] = successors[0].nodeAddress != replicas[2].nodeAddress;
        } else { repChanged[2] = true; }
        

        if (repChanged[0] || repChanged[1] || repChanged[2]){
            ht->deleteKey(key);
            clientCreate(key, value);
        }
    }

    for (auto& it : ht-> retTertiaryPairs()){
        string key = it.first;
        string value = it.second;
        vector<Node> replicas = findNodes(key);

        if (predecessors.size()>=2){
            repChanged[0] = predecessors[1].nodeAddress != replicas[0].nodeAddress;
        } else { repChanged[0] = true; }
        
        if (predecessors.size() >= 1){
            repChanged[1] = predecessors[0].nodeAddress != replicas[1].nodeAddress;
        } else { repChanged[1] = true; }

        repChanged[2] =  replicas[2].nodeAddress != memberNode->addr;

        if (repChanged[0] || repChanged[1] || repChanged[2]){
            ht->deleteKey(key);
            clientCreate(key, value);
        }
    }
}
