## Lecture 3

#### Question:

The Remus paper's Figure 6 suggests that less frequent checkpoints can lead to better performance. Of course, checkpointing only every X milliseconds means that up to X milliseconds of work are lost if the primary crashes. Suppose it was OK to lose an entire second of work if the primary crashed. Explain why checkpointing every second would lead to terrible performance if the application running on Remus were a Web server.

#### Answer:

As in the section 3 of the paper: 
Based on this analysis, we conclude that although Remus is efficient at state replication, it does introduce significant network delay, particularly for applications that exhibit poor locality in memory writes. Thus, applications that are very sensitive to network latency may not be well suited to this type of high availability service. 

If we increase the checkpointing interval as one second, which would require so much more time for checkpointing that the latency would be much longer. The application is a Web server which requires low latency, thus the performance would be greatly hurt.


## Lecture 4

#### Question:

Suppose tractserver T1 is temporarily unreachable due to a network problem, so the metadata server drops T1 from the TLT. Then the network problem goes away, but for a while the metadata server is not aware that T1's status has changed. During this time could T1 serve client requests to read and write tracts that it stores? If yes, give an example of how this could happen. If no, explain what mechanism(s) prevent this from happening.

#### Answer:

As stated in the section 3.2 of the paper, the client must use the latest TLT to contact with the tractservers, and as the tractserver T1 has a rather stale TLT, they can not share data as the TLTs does not match. Thus this TLT-base mechanism can prevent this from happening. 


## Lecture 5

#### Question:

Paxos Made Simple Suppose that the acceptors are A, B, and C. A and B are also proposers. How does Paxos ensure that the following sequence of events can't happen? What actually happens, and which value is ultimately chosen?

A sends prepare requests with proposal number 1, and gets responses from A, B, and C.
A sends accept(1, "foo") to A and C and gets responses from both. Because a majority accepted, A thinks that "foo" has been chosen. However, A crashes before sending an accept to B.
B sends prepare messages with proposal number 2, and gets responses from B and C.
B sends accept(2, "bar") messages to B and C and gets responses from both, so B thinks that "bar" has been chosen.

#### Answer:

Paxos only allowed committing previously accepted values. C has accepted "foo" from A, it would reply to B that i has accepted "foo" with a lower proposal number, thus B would choose "foo" for this proposal, which is also the committed value.


## Lecture 6

#### Question:

Suppose we have the scenario shown in the Raft paper's Figure 7: a cluster of seven servers, with the log contents shown. The first server crashes (the one at the top of the figure), and cannot be contacted. A leader election ensues. For each of the servers marked (a), (d), and (f), could that server be elected? If yes, which servers would vote for it? If no, what specific Raft mechanism(s) would prevent it from being elected?

#### Answer:

As stated in the section 5.4.1 of the paper, the candidate’s log is at least as up-to-date as any other log in that majority. Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs. If the logs have last entries with different terms, then the log with the later term is more up-to-date. If the logs end with the same term, then whichever log is longer is more up-to-date. 

For server (a): (a), (b), (e), (f) would vote (a)(4 / 6 of the cluster), thus (a) could be elected as leader.

For server (d): all the voters would vote (d), thus (d) could be elected as leader.

For server (f): only itself would vote (f), thus (f) could not be elected as leader.


## Lecture 8

#### Question:

Replication in the Harp File System Figures 5-1, 5-2, and 5-3 show that Harp often finishes benchmarks faster than a conventional non-replicated NFS server. This may be surprising, since you might expect Harp to do strictly more work than a conventional NFS server (for example, Harp must manage the replication). Why is Harp often faster? Will all NFS operations be faster with Harp than on a conventional NFS server, or just some of them? Which?

#### Answer:

As Harp uses a write-ahead log, which just saves the operation into the memory and does disk-writing in the background asynchronously, it is usually faster than a conventional non-replicated NFS server.

There are sometimes Harp is slower than NFS. For example, if the operation information is so big that surpassed the size threshold of log for Harp, Harp forces the completion of some writes, which would cost a long time.


## Lecture 9

#### Question:

Memory Coherence in Shared Virtual Systems. Answer this question using ivy-code.txt, which is a version of the code in Section 3.1 with some clarifications and bug fixes. You can see that the manager part of the WriteServer sends out invalidate messages, and waits for confirmation messages indicating that the invalidates have been received and processed. Suppose the manager sent out invalidates, but did not wait for confirmations. Describe a scenario in which lack of the confirmation would cause the system to behave incorrectly. You should assume that the network delivers all messages, and that none of the computers fail.

#### Answer:

For example, a server **A** reads the value **x** successfully and thus holds a copy of **x**. Then another server **B** writes the value **x** successfully while the manager does not wait for confirmation messages for the invalidates. Assume that the network is slow then, after a long time, we may still read the old value of **x** from the server A even though the writing is finished. Or in other words, the value of **x** is inconsistent after the writing is finished successfully.


## Lecture 10

#### Question:

Suppose that a simplified version of Treadmarks, called Dreadmarks, simply sent all modifications of variables between an acquire and a release to the next processor to acquire the same lock. No other modifications are sent. What changes does Treadmarks send that Dreadmarks does not? Outline a specific simple situation in which Treadmarks would provide more useful or intuitive memory behavior than Dreadmarks.

#### Answer:

Treadmarks only send the write notice to the next acquirer about which pages have been invalidated, only a request to invalidated page cause the process to request the modifications; but Dreadmarks send the actual modifications, it would be wasteful if the next process does not use them.


## Lecture 11

#### Question:

Ficus imagine a situation like the paper's Figure 1, but in which only Site A updates file Foo. What should Ficus do in that case when the partition is merged? Explain how Ficus could tell the difference between the situation in which both Site A and Site B update Foo, and the situation in which only Site A updates Foo.

#### Answer:

As stated in the section 2 of the paper, Ficus detects all types of conflicts using a mechanism known as a version vector and conflicts are detected by comparing version vectors from two file replicas. If only Site A updates file Foo, the version vector of A would will be a super set of B's, and if both Site A and Site B updates Foo, this condition does not hold.


## Lecture 12

#### Question:

Suppose we build a distributed filesystem using Bayou, and the system has a copy operation. Initially, file A contains "foo" and file B contains "bar". On one node, a user copies file A to file B, overwriting the old contents of B. On another node, a user copies file B to file A. After both operations are committed, we want both files to contain "foo" or for both files to contain "bar". Sketch a dependency check and merge procedure for the copy operation that makes this work. How does Bayou ensure that all the nodes agree about whether A and B contain "foo" or "bar"?

#### Answer:

First, the dependency check ensures that both files exist and the contents match some pattern(for example, "foo" or "bar").

Second, Bayou will maintain a global ordering when combined with the dependency checks, thus will ensure that all nodes agree on it. The first operation in the update log will occur and the other will not.

[Reference](https://github.com/ankushg/distributed-systems-go/blob/master/assignments/lec12.txt)


## Lecture 13

#### Question:

How soon after it receives the first file of intermediate data can a reduce worker start calling the application's Reduce function? Explain your answer.

#### Answer:

After the reducer receive all files of intermediate data from mappers as it needs to do a sort shuffle before reducing.


## Lecture 14

#### Question:

What applications can Spark support well that MapReduce/Hadoop cannot support?

#### Answer:

Iterative Algorithms - As Spark store data into memory thus iterative computations that visit same data several times will be much more faster on Spark;

Streaming Data


## Lecture 15

#### Question:

Suppose a Spanner server's TT.now() returns correct information, but the uncertainty is large. For example, suppose the absolute time is 10:15:30, and TT.now() returns the interval [10:15:20,10:15:40]. That interval is correct in that it contains the absolute time, but the error bound is 10 seconds. See Section 3 for an explanation TT.now(). What bad effect will a large error bound have on Spanner's operation? Give a specific example.

#### Answer:

Consider the following scenario:
- C_1 writes K1 to V1;
- C_2 reads K1;

As stated in the section 4.1.2("Commit Wait") of the paper, the leader will force C_1 to wait 10 seconds before committing. Thus C_2 have to wait 10 seconds to read the value V1.


## Lecture 16

#### Question:

Section 3.3 implies that a client that writes data does not delete the corresponding key from the Gutter servers, even though the client does try to delete the key from the ordinary Memcached servers (Figure 1). Explain why it would be a bad idea for writing clients to delete keys from Gutter servers.

#### Answer:

As the Gutter servers are already backups for failed service, the deletion of keys would cause more traffic for the servers which would harm read performance.


## Lecture 17

#### Question:

Briefly explain why it is (or isn't) okay to use relaxed consistency for social applications (see Section 4). Does PNUTS handle the type of problem presented by Example 1 in Section 1, and if so, how?

#### Answer:

As illustrated in Example 1 in Section 1, it is not okay to use relaxed consistency for social applications when user wants sequential actions, for example, forbidding mom for a specific photo album then uploading a photo into it. 

PNUTS handle the problem by (1) using per-record timeline consistence, (2) allowing applications to specify the degree of read consistency. 


## Lecture 18

#### Question:

Suppose Dynamo server S1 is perfectly healthy with a working network connection. By mistake, an administrator instructs server S2 to remove S1 using the mechanisms described in 4.8.1 and 4.9. It takes a while for the membership change to propagate from S2 to the rest of the system (including S1), so for a while some clients and servers will think that S1 is still part of the system. Will Dynamo operate correctly in this situation? Why, or why not?

#### Answer:

Yes. As Dynamo used gossip protocol to propagate membership changes, some clients and servers are still able to communicate with S1.


## Lecture 19

#### Question:

Building distributed systems in the real world have both technical challenges (e.g., dealing with bad data) and non-technical ones (e.g., how to structure a team). Which do you think is harder to get right? What examples can you cite from your personal experience?

#### Answer:

I think it is really hard to organize the data because they may be structured, semi-structured or not structured and may come from various sources and have different semantic meanings. You have to carefully design the schema for them and collaborate with people from various areas. Once I was working on a project that did some cohort analysis using data from external sources, I came across tens of questions during ingesting data, calculating and displaying that I could not imagine before this.


## Lecture 20

#### Question:

Starting at the bottom-left of page 310, the paper mentions that a participant writes new versions to disk twice: once before replying to a prepare message, and once after receiving a commit message. Why are both writes necessary? What could go wrong if participants replied to the prepare without writing the disk, instead only writing the disk after receiving a commit message?

#### Answer:

If the participant crashed, it could not know whether it has accepted the `prepare` message. 


## Lecture 21

#### Question:

The paper mentions that, after a server commits a transaction, the server sends out invalidation messages to clients that are caching data written by that transaction. It may take a while for those invalidations to arrive; during that time, transactions at other clients may read stale cached data. How does Thor cope with this situation?

#### Answer:

The client has recevied the `prepare` message for this to-be-committed transaction and responsed `ok`, thus it will abort the transaction reading stale cached data.


## Lecture 22

#### Question:

Consider a Kademlia-based key-value store with a million users, with non-mutable keys: once a key is published, it will not be modified. The k/v store experiences a network partition into two roughly equal partitions A and B for 1.5 hours.

X is a very popular key. Would nodes in both A and B likely be able to access X's value (1) during the partition? (2) 10 minutes after the network is joined? (3) 25 hours after the network is joined?

(optional) Would your answer change if X was an un-popular key?

#### Answer:

As X is a very popular key, it will be replicated at several servers, thus it can be accessed in these three situations. But if X is an un-popular key, it can not be accessed during the partition(if it was only stored in one server) and may not be accessed 10 minutes after the network is joined as the fresh were done every hour. It can be accessed 25 hours after the network is joined.