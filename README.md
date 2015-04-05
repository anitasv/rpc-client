rpc-client
==========

General purpose client, which can do load balancing, fault tracking, and latency minimization etc.
This does not implement any actual RPC mechanisms. You may use Apache Thrift, or other technologies
for that purpose.

First write an implementation for RpcService, tie one instance per backend host. Multiple connections
to the same host is expected to be managed inside the client implementation of RpcService. LeastLoaded
is one of the mechanisms provided right now to load balance across multiple RpcServices.

For example:
```java
  ImmutableList<HostPort> servers = config.getServiceBackends();
  Immutable<RpcService<GeoRequest, GeoResponse>> backends = 
      ImmutableList.copyOf(Lists.transform(servers, GeoThriftRpcService::new));
  ExecutorService executor = Executors.newCachedThreadPool();
  
  RpcService<GeoRequest, GeoResponse> uberBackend = new LeastLoaded(backends, executor);
```

LeastLoaded and Preferred are RpcServices by themselves, so that we can make meta balancing like:
```java
Primary = List of 10-servers
Secondary = List of 900 servers.
Backend1 = LeastLoaded(Primary)
Backend2 = LeastLoaded(Secondary)
Final = Preferred(Backend1, Backend2);
```

So that majority of the time we will loadbalance across some 10 servers, and if all of them goes
unavailable then move to list of secondary servers. The reason for this split is to be able to
manage number of overall connections to reduce pressure over the network.

Least loaded is especially useful if you have multiple backends with different latency characteristics,
but similar capacity. For example if you have three backends with 10ms, 20ms, and 30ms latency for
each. Then you would expect first server to handle around 100 qps, second 50 qps, and third 33 qps. 
Giving an overall 183 qps. One of the test in LeastLoadedTest checks precisely this, but gets an 
overall 180qps, with 78-62-43 split instead of the perfect 100-50-33 on a poisson distributed input. 

But in reality individual capacity of each server might differ because of the number of CPUs in 
each machine. In which case you would need a Weighted Least Loaded machinery. One way to do this
would be to use small integer weights, and add each backend multiple times. But this will cause 
some other optimizations to go wrong. To circumvent this problem, we need direct Weighted Least Loaded
class. 

If you have servers which return responses faster when in failure, least loaded itself may not
be the best strategy. You may want to consider Round Robin. Also sometimes Weighted Round Robin.

Some of these may also support Hedging feature in which, if a request takes more than a specified
time (usually around 95 percentile), it is retried with another server. This is not possible to
be implemented at a higher level because the abstraction looses which host pair we were talking to.
Sometimes it may be necessary to do at a different level if the topology wants to force a hedged
request go to a different switch, or even a different rack for whatever reason. Sometimes least loaded
may need to be counted at a switch level, and not a host level. In which case you may meta balance
least loaded over a bunch of round robin boxes. 
```java
  switch1 = RoundRobin(box1, box2, box3);
  switch2 = RoundRobin(box4, box5, box6);
  switch3 = RoundRobin(box7, box8);
  
  uberBackend = LeastLoaded(switch1, switch2, switch3)
```

Sometimes you may even have different class of switches, and or some switches already allocates part
of its load to other servers which is unrelated to this backend that even this won't work as well.

We are also thinking about how to do this whole management automatically, and reconfigure automatically
as and when the system detects each node can do more or less than it originally promised. But we are
leaving this topic out of scope of this library. And assuming the topology graph is computed by an
external program and fed into this system.

