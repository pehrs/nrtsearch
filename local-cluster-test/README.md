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

$ ./gw-start.sh

$ docker ps
CONTAINER ID   IMAGE                 COMMAND                  CREATED         STATUS         PORTS                                                           NAMES
5012c44bf24c   grpc-gateway:latest   "./build/install/nrt…"   9 minutes ago   Up 9 minutes   0.0.0.0:7100-7101->7100-7101/tcp, :::7100-7101->7100-7101/tcp   replica1
981206e02991   grpc-gateway:latest   "./bin/http_wrapper-…"   9 minutes ago   Up 9 minutes   0.0.0.0:6080->6080/tcp, :::6080->6080/tcp                       gw
0cf572963678   grpc-gateway:latest   "./build/install/nrt…"   9 minutes ago   Up 9 minutes   0.0.0.0:6000-6001->6000-6001/tcp, :::6000-6001->6000-6001/tcp   primary
```

# lucene client

Use the [`lucene-client.sh`](lucene-client.sh) script to run client commands

# Sample index test_idx

Run the [`test_idx-init.sh`](test_idx-init.sh) script to create the index.

Look at the [`search.sh`](search.sh) for a sample on how to do a search REST call.


# Cleanup

Once your test is done just kill and remove the docker containers:

```
$ docker kill primary replica1 gw

$ docker rm primary replica1 gw
```

