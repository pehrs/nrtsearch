# Simple cluster setup test 

These scripts will use docker to setup one primary, one replica and a grpc-rest-gw

# Setup

Please make sure you have buildt the nrtsearch server and grpc gw before running any of the scripts below.

```
./gradlew clean installDist test buildGrpcGateway
```

# Run

```
$ ./primary-server-start.sh

$ ./replica1-server-start.sh

$ ./primary-gw-start.sh

$ ./replica1-gw-start.sh

$ docker ps
CONTAINER ID   IMAGE                 COMMAND                  CREATED          STATUS          PORTS                                                           NAMES
aeaf1876a8c2   grpc-gateway:latest   "./build/install/nrt…"   2 seconds ago    Up 1 second     0.0.0.0:7100-7101->7100-7101/tcp, :::7100-7101->7100-7101/tcp   replica1
6d9c57d78c6e   grpc-gateway:latest   "./build/install/nrt…"   15 seconds ago   Up 14 seconds   0.0.0.0:6000-6001->6000-6001/tcp, :::6000-6001->6000-6001/tcp   primary
bb30fa5164e8   grpc-gateway:latest   "./bin/http_wrapper-…"   40 minutes ago   Up 40 minutes   0.0.0.0:6081->6080/tcp, :::6081->6080/tcp                       replica1-gw
910129ba67a4   grpc-gateway:latest   "./bin/http_wrapper-…"   41 minutes ago   Up 41 minutes   0.0.0.0:6080->6080/tcp, :::6080->6080/tcp                       primary-gw
```

# lucene client

Use the [`lucene-client.sh`](lucene-client.sh) script to run client commands

# Sample index test_idx

Run the [`test_idx-init.sh`](test_idx-init.sh) script to create the index.

Look at the [`search.sh`](search.sh) for a sample on how to do a search REST call.

IF you restart the servers you need to also start the index:

```
$ ./test_idx-startIndex.sh
```

# Cleanup

Once your test is done just kill and remove the docker containers:

```
$ docker kill primary replica1 primary-gw replica1-gw

$ docker rm primary replica1 primary-gw replica1-gw
```

