
# README - Fault-Tolerant Key-Value Store

## What this is
A distributed fault-tolerant key-value store running on top of an emulated network layer\
This is a project for the Distributed System course from UIUC.

## Functions supported  
- A key-value store supporting ​CRUD operations​ (Create, Read, Update, Delete).
- Load-balancing​ (via a consistent hashing ring to hash both servers and keys).
- Fault-tolerance​ up to 2 failures (by replicating each key 3 times to 3 successive nodes in the ring, starting from the first node at or to the clockwise of the hashed key).
- Quorum consistency level​ for both reads and writes (at least 2 replicas).
- Stabilization​ after failure (recreate 3 replicas after failure)

## Architecture 
A three-layer framework that runs multiple copies of peers within one process running a single-threaded simulation engine.\
the three layers are:
1. the lower EmulNet (network)
2. the P2P middle layer consisting of multiple peer nodes where each peer has a MP1Node (failure detection membership protocol) and a MP2Node (the key-value store). At each peer node, the key-value store talks to the membership protocol and receives from it the membership list. It then uses this to maintain its view of the virtual ring. Periodically, each node engages in the membership protocol to try to bring its membership up to date. 
3. the application layer that drives the application

![Image of 3 layer architecture](/architecture.jpg)


## Algorithms used 
### Gossip Membership Protocol for Failure Detection
Each node keeps a heartbeat of its own which increment by 1 per second.\
Each node also keeps a membership table with the most recently received heartbeat for each node.\
Periodically, each process sends over this entire membership table to randomly selected few in its member list (neighbors).\ 
When receive a gossip, compare each row with its own. If heartbeat is larger in the gossip, update its own row with address, largest heartbeat, current local time. \
If the heartbeat has not increased for more than T failed seconds, mark it as having failed.\ 
Wait for another T_cleanup seconds, then remove failed process from memberlist. \

This protocol satisfies the following: 
1. Completeness all the time: every non-faulty process must detect every node join, failure, and leave
2. Accuracy of failure detection when there are no message losses and message delays are small. When there are message losses, completeness must be satisfied and accuracy must be high. It must achieve all of these even under simultaneous multiple failures.


### Consistent Hashing on Distributed Hashing Table
Each node maintains a view of the virtual ring(Distributed Hashing Table) on which all nodes has a position determined by its hash value.\
Each data item identified by a key is assigned to a node by hashing the data item’s key to yield its position on the ring, and then walking the ring clockwise to find the first node with a position larger than the item’s position. This node is deemed the coordinator for this key and 2 additional replicas are also created on its 2 neighors.

## References
A Gossip-Style Failure Detection Service: https://www.cs.cornell.edu/home/rvr/papers/GossipFD.pdf \
Chord: A Scalable Peer-to-peer Lookup Service for Internet Applications: https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf \
Cassandra - A Decentralized Structured Storage System: https://www.cs.cornell.edu/projects/ladis2009/papers/lakshman-ladis2009.pdf  \
CS425 Distributed Systems: https://courses.grainger.illinois.edu/cs425/fa2021/  
 
## Testing

You can run this test suite locally like this:

```
$ chmod +x KVStoreTester.sh
$ ./KVStoreTester.sh
```

The `chmod` part only needs to be done once to allow the script to be executed. It's okay if the `chmod` command gives an error message. In that case you can do this instead:

```
$ bash ./KVStoreTester.sh
```

### How do I run the CRUD tests separately?

First, compile your project:

```
$ make clean
$ make
```

Then use one of these invocations:

```
$ ./Application ./testcases/create.conf
$ ./Application ./testcases/delete.conf
$ ./Application ./testcases/read.conf
$ ./Application ./testcases/update.conf
```

You may need to do `make clean && make` in between tests to make sure you have a clean run.
