## Lecture 3

#### Question:
```
The Remus paper's Figure 6 suggests that less frequent checkpoints can lead to better performance. Of course, checkpointing only every X milliseconds means that up to X milliseconds of work are lost if the primary crashes. Suppose it was OK to lose an entire second of work if the primary crashed. Explain why checkpointing every second would lead to terrible performance if the application running on Remus were a Web server.
```

#### Answer:

As in the section 3 of the paper: 
```
Based on this analysis, we conclude that although Remus is efficient at state replication, it does introduce significant network delay, particularly for applications that exhibit poor locality in memory writes. Thus, applications that are very sensitive to network latency may not be well suited to this type of high availability service. 
```

If we increase the checkpointing interval as one second, which would require so much more time for checkpointing that the latency would be much longer. The application is a Web server which requires low latency, thus the performance would be greatly hurt.

## Lecture 4

#### Question:
```
Flat Datacenter Storage Suppose tractserver T1 is temporarily unreachable due to a network problem, so the metadata server drops T1 from the TLT. Then the network problem goes away, but for a while the metadata server is not aware that T1's status has changed. During this time could T1 serve client requests to read and write tracts that it stores? If yes, give an example of how this could happen. If no, explain what mechanism(s) prevent this from happening.
```

#### Answer:

As stated in the section 3.2 of the paper, the client must use the latest TLT to contact with the tractservers, and as the tractserver T1 has a rather stale TLT, they can not share data as the TLTs does not match. Thus this TLT-base mechanism can prevent this from happening.