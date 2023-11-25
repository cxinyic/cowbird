#### Hash read benchmark
1. make
on compute node & memory node
```console
cxinyic@node-0:~/cowbird/hash_read/hash_read_client$ make 
```
```console
cxinyic@node-1:~/cowbird/hash_read/hash_read_server$ make 
```
2. launch the server on the memory node
For baselines: run ./baseline_server; for cowbird: run ./cowbird_server;
```console
cxinyic@node-0:~/cowbird/hash_read/hash_read_server$ ./baseline_server -h
Usage: ./baseline_server -n thread_number [-s data_size] [-t task_type]
Options:
  -n  Set the number of worker threads (mandatory)
  -s  Set the size of processed data (optional), default size: 64B
  -a  Set the IP address (optional)
  -t  Set the task type (optional), default value: 0, (0/1/2: not two-sided; 3: two-sided)
  -h  Show this help message
```
```console
// Eg: run one-sided sync baseline, one thread, data size 64
cxinyic@node-1:~/cowbird/hash_read/hash_read_server$ ./baseline_server -n 1 -s 64 -t 1 
```
```console
// Eg: run cowbird, two threads, data size 256 
cxinyic@node-0:~/cowbird/hash_read/hash_read_server$ ./cowbird_server -n 2 -s 256
```
3. launch the client on the compute node
For baselines: run ./baseline_client; for cowbird: run ./cowbird_client;
```console
// Eg: run one-sided async baseline, one thread, data size 64 (type 2 represents one-sided async)
cxinyic@node-0:~/cowbird/hash_read/hash_read_client$ ./baseline_client -n 1 -s 64 -t 2 
```
```console
// Eg: run cowbird, two threads, data size 256 
cxinyic@node-0:~/cowbird/hash_read/hash_read_client$ ./cowbird_client -n 2 -s 256
```
4. client will show the results and exit, please shutdown the server manually(^C)
```console
cxinyic@node-0:~/cowbird/hash_read/hash_read_client$ ./baseline_client -n 1 -s 64 -t 2
RdmaClient: before connect to server
RdmaClient: after connect to server
RdmaClient: after register RDMA buffers
RdmaClient: after recv server buffer mr, remote server addr is 7fa5abba1010
RDMAClient: generating keys...
RDMAClient: finished generating

Start executing one-sided async baseline
RDMAClient: thread 0 is working

Hash read result: ---------------
one-sided async, data size is 64
overall time is 5 sec
thread 0 finished 19568800 ops
finished 19568800 ops, throughtput is (3.913760) MOps
```
```console
cxinyic@node-1:~/cowbird/hash_read/hash_read_server$ ./baseline_server -n 1 -s 64 -t 2
RDMAServer: after malloc buffer pool, addr is 0x7fa5abba1010
RDMAServer: waiting for connection from RDMAClient ...
RdmaServer: after connect to client
RdmaServer: after register RDMA buffers
RdmaServer: after send server buffer mr, remote server addr is 0x7fa5abba1010
RdmaServer will do nothing for task type 0
^C
```
```console
cxinyic@node-0:~/cowbird/hash_read/hash_read_client$ ./cowbird_client -n 1 -s 64
CowbirdClient: before connect to server
CowbirdClient: after connect to client
CowbirdClient: after register RDMA buffers
CowbirdClient: start buffer info exchange
CowbirdClient: after recv server buffer mr, remote server addr is 7f7b6f846010
CowbirdClient: finish buffer info exchange
CowbirdClient: generating keys...
CowbirdClient: finished generating

CowbirdClient: thread 0 is working

Hash read result: ---------------
Cowbird, data size is 64
overall time is 5 sec
thread 0 finished 44738977 ops
finished 44738977 ops, throughtput is (9.057795) MOps
---------------------------------
```
```console
cxinyic@node-1:~/cowbird/hash_read/hash_read_server$ ./cowbird_server -n 1 -s 64
CowbirdServer: waiting for connection from RDMAClient ...
CowbirdServer: after connect to client
CowbirdServer: after register RDMA buffers
CowbirdServer: start buffer info exchange
CowbirdServer: after recv client cowbird buffer info
CowbirdServer: after send server buffer mr, addr is 7f7b6f846010
CowbirdServer: finish buffer info exchange
^C
```
5. Some small things: when client exits, you might see "mlx5: host_unknown: got completion with error:" on the server, it is fine since the server is still trying to poll requests from the client and the client exits. 
6. Performance upperbound: some machines on cloudlab only have 25G RDMA NICs(like xl170). One will see that the cowbird reaches this limit sooner with 1 or 2 threads if the data size is large(128B/256B). 
