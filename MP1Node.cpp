/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions. (Revised 2020)
 *
 *  Starter code template
 **********************************/

#include "MP1Node.h"
#include "Member.h"
#include <cstddef>

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */

MP1Node::MP1Node( Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = new Member;
    this->shouldDeleteMember = true;
	memberNode->inited = false;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member* member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for( int i = 0; i < 6; i++ ) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->shouldDeleteMember = false;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {
    if (shouldDeleteMember) {
        delete this->memberNode;
    }
}

/**
* FUNCTION NAME: recvLoop
*
* DESCRIPTION: This function receives message from the network and pushes into the queue
*                 This function is called by a node to receive messages currently waiting for it
*/
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
* FUNCTION NAME: nodeStart
*
* DESCRIPTION: This function bootstraps the node
*                 All initializations routines for a member.
*                 Called by the application layer.
*/
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }
    //printMemberList(memberNode->memberList);
    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
    * This function is partially implemented and may require changes
    */
	//int id = *(int*)(&memberNode->addr.addr);
	//int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
	// node is up!
	// memberNode->nnb = 0; //number of neighbors, can use the size of the memberlist table as nnb 
	// memberNode->heartbeat = 0; // use memberlist to record my own heartbeat. 
	//memberNode->pingCounter = TFAIL;
	//memberNode->timeOutCounter = -1;
	initMemberListTable(memberNode);

    return 0;
}


/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	
#ifdef DEBUGLOG
    static char s[1024];
#endif
   

    if ( 0 == strcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        // format {MessageHdr Address Heartbeat} 
        size_t msgSize = sizeof(Address)+sizeof(long);
        MessageHdr* msg = (MessageHdr *) malloc((sizeof(MessageHdr)+msgSize)*sizeof(char));
        msg->msgType = JOINREQ;
        msg->msgSize = msgSize;
        memcpy((char*)(msg+1), &memberNode->addr.addr, sizeof(Address));
        memcpy((char*)(msg+1)+ sizeof(Address), &memberNode->myPos->heartbeat,sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        // int ENsend(Address *myaddr, Address *toaddr, char *data, int size);
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msg->msgSize + sizeof(MessageHdr));
        free(msg);
    }

    return 1;

}

/**
* FUNCTION NAME: finishUpThisNode
*
* DESCRIPTION: Wind up this node and clean up state
*/
int MP1Node::finishUpThisNode(){
    /*
     * Your code goes here
     */
    memberNode->bFailed = false;
    memberNode->inited = false;
	memberNode->inGroup = false;
    memberNode->memberList.clear();
    return 1;
}

/**
* FUNCTION NAME: nodeLoop
*
* DESCRIPTION: Executed periodically at each member
*                 Check your messages in queue and perform membership protocol duties
*/
// handle messages and send heartbeats

void MP1Node::nodeLoop() {
    //printAddress(&memberNode->addr);
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();
    //cout<< "finished receiving messages.. \n";
    //printMemberList(memberNode->memberList);

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    // decide who failed; who to cleanup; increment own heartbeat and send out gossip;
    nodeLoopOps();
    //printAddress(&memberNode->addr);
    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
 // introducer receive JOINREQ, reply with JOINREP 
 // all nodes receive GOSSIP 
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
        // recvCallBack(void *env, char *data, int size )
        // env parameter redundant? 
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    /*
    * Your code goes here    */
    /*
    if receive JOINREQ, add peer to membership list, reply with JOINREP 
    if receive JOINREP, copy the membershiplist
    if receive GOSSIP, update the membershiplist 
    */ 
    MessageHdr* msg = (MessageHdr*) data;
    switch(msg->msgType) {
        case JOINREQ:{
            handleJoinRequest(msg);
            break;
        }
        case JOINREP:{
            handleJoinReply(msg);
            break;
        }

        case GOSSIP:{
            /*
            printAddress(&memberNode->addr);
            cout<<"received GOSSIP of size "<<msg->msgSize/sizeof(MemberListEntry)<<'\n';
            printMessage(msg);
            */
            handleGossip(msg); 
            break;
        }
        default:
#ifdef DEBUGLOG
            log->LOG(&memberNode->addr, " Received unknown message type. Exit.");
#endif
            exit(999); 
    }   
    free(msg);
    return true;
}


void MP1Node::addMemberListToMsg(MessageHdr* msg){
// copy memberNode -> memberList after message header 
    char * cursor = (char*) (msg+ 1); 
    for(size_t i = 0; i != memberNode->memberList.size(); i++) {
        memcpy(cursor, &memberNode->memberList[i], sizeof(MemberListEntry));
        /*
        if (((MemberListEntry*)cursor) -> id == 0){
            cout << "added null address to message"<<'\n';
            exit(666);
        }*/
        cursor += sizeof(MemberListEntry);
    }
}

// create a JOINREP message, 
void MP1Node::handleJoinRequest(MessageHdr* msg) {
    Address *addr =(Address *) (msg+1); 
    int id = *(int*) addr->addr;
    int port = *(short*) &addr->addr[4]; 
    long heartbeat = *(long *) (addr + 1);
    MemberListEntry newEntry = MemberListEntry(id, port, heartbeat, par->getcurrtime());
    memberNode -> memberList.push_back(newEntry);
#ifdef DEBUGLOG
    log->logNodeAdd(&memberNode->addr, addr);
#endif

    size_t msgSize = sizeof(MemberListEntry) * memberNode->memberList.size();
    MessageHdr* replyMsg = (MessageHdr *) malloc(sizeof(MessageHdr) + msgSize);
    replyMsg->msgType = JOINREP;
    replyMsg->msgSize = msgSize;
    addMemberListToMsg(replyMsg);
    
    
    emulNet->ENsend(&memberNode->addr, addr, (char *)replyMsg, replyMsg->msgSize + sizeof(MessageHdr));
    free(replyMsg);
}

void MP1Node::printMessage(MessageHdr* msg){   
    printf("Message Type: %d, Message size %d \n", msg->msgType, int(msg->msgSize/sizeof(MemberListEntry)));
    MemberListEntry * curEntry =(MemberListEntry *) (msg+1);
    for (size_t i = 0; i< msg->msgSize/sizeof(MemberListEntry); i ++ ){
        printf("id: %d, port:%hu, heartbeat:%ld, timestamp: %ld \n", curEntry->id, curEntry->port, curEntry->heartbeat,curEntry->timestamp);
        curEntry ++;
    }

}

void MP1Node::printMemberList(vector<MemberListEntry> &memberList){   
    /*
    cout<< "current memberlist of ";
    printAddress(&memberNode->addr);
    cout <<'\n';*/
    for (auto it = memberList.begin() ; it != memberList.end(); it++){
        printf("id: %d, port:%d, heartbeat:%ld, timestamp: %ld \n", it->id, it->port, it->heartbeat,it->timestamp);
    }

}


// merge new memberList with current memberList 
// if any heartbeat is increased, record it with new timestamp
// if see any new process, add new memberlistEntry and log add node  
void MP1Node::updateMemberList(MessageHdr* msg){
    MemberListEntry * curEntry =(MemberListEntry *) (msg+1);
    for (size_t i = 0; i< msg->msgSize/sizeof(MemberListEntry); i ++ ){
        /*
        if (curEntry->id == 0){
            printAddress(&memberNode->addr);
            cout<< "received null address ";
            cout<< i<< "/" << msg->msgSize/sizeof(MemberListEntry)<<'\n';
            //printMessage(msg);
            exit(777);
        }*/
        bool found = false;
        for (auto it = memberNode->memberList.begin() ; it != memberNode->memberList.end(); it++){
            if (it->id == curEntry->id && it->port == curEntry->port){
                found = true; 
                if (it->heartbeat < curEntry->heartbeat){
                    //cout << "received new heartbeat "<<'\n';
                    it->heartbeat = curEntry->heartbeat;
                    it->timestamp = par->getcurrtime();
                }
            }
        }
        if (!found){
            //cout << "discovered new process"<<'\n';
            MemberListEntry newEntry = MemberListEntry(*curEntry);
            /*
            if (newEntry.id == 0){
                cout<< "adding null address process to memberlist"<<'\n';
                exit(777);
            }*/
            newEntry.timestamp = par->getcurrtime();
            memberNode -> memberList.push_back(newEntry);
#ifdef DEBUGLOG
            Address newAddr = makeAddress(curEntry->id, curEntry->port);             
            log->logNodeAdd(&memberNode->addr, &newAddr);
#endif
        }
        curEntry ++;
    }
    return;
}

// copy the membershiplist, log add? (wait for piazza answer)  
void MP1Node::handleJoinReply(MessageHdr* msg) {
    memberNode->inGroup = true;
    /*
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "JOINREP received");
#endif
*/
    updateMemberList(msg); 
}

// update the membershiplist,
void MP1Node::handleGossip(MessageHdr* msg) {
    updateMemberList(msg);
    //cout << "finished handling GOSSIP"<<'\n';
}

// generate an Address object from id, port 
Address MP1Node::makeAddress(int id, short port){
    //cout << "making address from "<<id<<" "<<port<<'\n';
    Address addr;
    memcpy(&addr.addr, &(id), sizeof(int));
    memcpy(&addr.addr[4], &(port), sizeof(short));
    return addr;
}



void MP1Node::addSingleEntryToMsg(MessageHdr* msg, MemberListEntry* cursor, MemberListEntry* entry){
    memcpy(cursor, entry, sizeof(MemberListEntry));
    //printf("adding to message ... id: %d, port:%d, heartbeat:%ld, timestamp: %ld \n", entry->id, entry->port, entry->heartbeat, entry->timestamp);
    //printf("added entry ... id: %d, port:%d, heartbeat:%ld, timestamp: %ld \n", cursor->id, cursor->port, cursor->heartbeat, cursor->timestamp );
    (msg->msgSize) += sizeof(MemberListEntry);
}

// get the address of a random neighbor that's not myself
Address MP1Node::getRandomNeighbor(){
    auto it = memberNode->memberList.cbegin();
    do{
	    int random = rand() % memberNode->memberList.size();
	    advance(it, random);
    } while (memberNode->myPos == it);
    //cout << "found random neighbor to send message to"<<'\n';
	return makeAddress(it->id,it->port);
}

/**
* FUNCTION NAME: nodeLoopOps
*
* DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
*                 the nodes
*                 Propagate your membership list
*/
//decide who failed; who to cleanup; increment own heartbeat and send out a gossip to a random neighbor ;

void MP1Node::nodeLoopOps() {
    
    /*
     * Your code goes here
     */
    // prepare GOSSIP
    MessageHdr* msg = (MessageHdr *) malloc(sizeof(MessageHdr) + sizeof(MemberListEntry) * memberNode->memberList.size());
    msg->msgType = GOSSIP;
    msg->msgSize = 0;
    MemberListEntry * cursor = (MemberListEntry*) (msg+ 1); 
    
    for (auto it = memberNode->memberList.begin() ; it != memberNode->memberList.end();){
        int timeElapsed = par->getcurrtime() - it->timestamp;
        Address curAddr = makeAddress(it->id, it->port);
        if (0 == memcmp(memberNode -> addr.addr, curAddr.addr, sizeof(Address))){
            //cout<<"found myself"<<'\n';
            //printAddress(&curAddr);
            //printAddress(&memberNode ->addr);
            //increase heartbeat, add myself to gossip message
            (it -> heartbeat)++; 
            it -> timestamp = par->getcurrtime();
            addSingleEntryToMsg(msg, cursor, &(*it));
            ++it; 
            ++cursor;
        }
        else if (timeElapsed > TREMOVE){
            // remove MemberListEntry from memberlist 
#ifdef DEBUGLOG
            log->logNodeRemove(&memberNode->addr, &curAddr);  
#endif  
            it = memberNode->memberList.erase(it);
        } 
        else if (timeElapsed <= TFAIL){
            // add entry to gossip message
            addSingleEntryToMsg(msg, cursor, &(*it));
            ++it; 
            ++cursor;
        }
        // if timeElasped > TFAIL, stop gossiping about the process
        else{
            ++it;
        };
        
    }
    //printMessage(msg);
    // Select a random neighbor to send GOSSIP to 
    Address randomNeighbor = getRandomNeighbor();
    /*
    printAddress(&memberNode->addr);
    cout<<"sending GOSSIP of size "<< msg->msgSize/sizeof(MemberListEntry)<<"to ";
    printAddress(&randomNeighbor);
    cout <<'\n';*/
    emulNet->ENsend(&memberNode->addr, &randomNeighbor , (char *)msg, msg->msgSize + sizeof(MessageHdr));
    free(msg);
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
    // Add myself to my memberList
    int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);
    MemberListEntry myself = MemberListEntry(id, port, INIT_HEARTBEAT, par->getcurrtime());
    memberNode->memberList.push_back(myself);
    memberNode->myPos = memberNode->memberList.begin();
#ifdef DEBUGLOG
    log->logNodeAdd(&memberNode->addr, &memberNode->addr);
#endif
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d ",  addr->addr[0],addr->addr[1],addr->addr[2], addr->addr[3], *(short*)&addr->addr[4]) ;    
}
