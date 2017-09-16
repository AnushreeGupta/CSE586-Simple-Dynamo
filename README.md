# Simple Dynamo

This project implements a simplified version of Dynamo with the below three main pieces.
1. Partitioning
2. Replication 
3. Failure handling.

The main goal is to provide both availability and linearizability at the same time. In other words, the implementation should always perform read and write operations successfully even under failures. At the same time, a read operation always returns the most recent value. 

The following guidelines are kept in mind for the content provider based on the design of Amazon Dynamo:

#### A. Membership

1. Just as the original Dynamo, every node knows all other nodes in the system and also knows exactly which partition belongs to which node.

#### B. Request routing

1. Unlike Chord, each Dynamo node knows all other nodes in the system and also knows exactly which partition belongs to which node.
2. Under no failures, a request for a key is directly forwarded to the coordinator (i.e., the successor of the key), and the coordinator should be in charge of serving read/write operations.

#### C. Quorum replication

1. For linearizability, implement a quorum-based replication used by Dynamo.
2. Note that even though the original design does not provide linearizability, this app should.
3. The replication degree N should be 3. This means that given a key, the key’s coordinator as well as the 2 successor nodes in the Dynamo ring should store the key.
4. Both the reader quorum size R and the writer quorum size W should be 2.
5. The coordinator for a get/put request should always contact other two nodes and get a vote from each (i.e., an acknowledgement for a write, or a value for a read).
6. For write operations, all objects can be versioned in order to distinguish stale copies from the most recent copy.
7. For read operations, if the readers in the reader quorum have different versions of the same object, the coordinator should pick the most recent version and return it.

#### D. Chain replication

1. Another replication strategy you can implement is chain replication, which provides linearizability.
2. If you are interested in more details, please take a look at the following paper: http://www.cs.cornell.edu/home/rvr/papers/osdi04.pdf
3. In chain replication, a write operation always comes to the first partition; then it propagates to the next two partitions in sequence. The last partition returns the result of the write.
4. A read operation always comes to the last partition and reads the value from the last partition.

#### E. Failure handling

1. Handling failures should be done very carefully because there can be many corner cases to consider and cover.
2. Just as the original Dynamo, each request can be used to detect a node failure.
3. For this purpose, you can use a timeout for a socket read; you can pick a reasonable timeout value, e.g., 100 ms, and if a node does not respond within the timeout, you can consider it a failure.
4. Do not rely on socket creation or connect status to determine if a node has failed. Due to the Android emulator networking setup, it is not safe to rely on socket creation or connect status to judge node failures. Please use an explicit method to test whether an app instance is running or not, e.g., using a socket read timeout as described above.
5. When a coordinator for a request fails and it does not respond to the request, its
successor can be contacted next for the request.
