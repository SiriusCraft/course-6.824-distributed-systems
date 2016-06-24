## Lecture 2

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