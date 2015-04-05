rpc-client
==========

General purpose client, which can do load balancing, fault tracking, and latency minimization etc.
This does not implement any actual RPC mechanisms. You may use Apache Thrift, or other technologies
for that purpose.

First write an implementation for RpcService, tie one instance per backend host. Multiple connections
to the same host is expected to be managed inside the client implementation of RpcService. LeastLoaded
is one of the mechanisms provided right now to load balance across multiple RpcServices.

The expectation is we can make meta balancing like:
Final = Preferred(Backend1, Backend2);
Backend1= LeastLoaded(Primary)
Backend2= LeastLoaded(Secondary)
Primary = List of 10-servers
Secondary = List of 100 servers.

So that majority of the time we will loadbalance across some 10 servers, and if all of them goes
unavailable then move to list of secondary servers. The reason for this split is to be able to
manage number of overall connections to reduce pressure over the network.
