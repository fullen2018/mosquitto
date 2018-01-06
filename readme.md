Mosquitto with cluster
=================

Mosquitto with cluster implement a non-centralized cluster.
The cluster is a full autonomy mosquitto cluster without any leader or key service,
to make the system with a high availability.

## Usage

Install mosquitto on all of the nodes and write the addresses into mosquitto.conf,<br>
e.g.<br>
node_name node1<br>
node_address 192.168.1.1:1883<br>
<br>
node_name node2<br>
node_address 192.168.1.2:1883<br>

Then config the loadbalancer, take above adresses as real server address.

## Installing

Inside config.mk, comment "WITH_BRIDGE:=yes", and uncomment "WITH_CLUSTER:=yes".<br>

See <http://mosquitto.org/download/> for details on installing binaries for
various platforms.

## Cluster Specification

Broadcast clients' subscription/unsubscription to each other brokers inside the cluster.<br>

Some private messages was introduced in order to support cluster session and retain message.<br>
i.e.<br>
PRIVATE SUBSCRIBE<br>
PRIVATE RETAIN<br>
SESSION REQ<br>
SESSION RESP<br>
<br>
For cluster session, SESSION REQ would be broadcast for each client connect, and if
clean session set to false, remote cluster would return this client's session include
subscription, incomplete publishes with QoS>0 inside SESSION RESP.
<br><br>
For retain message, PRIVATE SUBSCRIBE would be broadcast for each client subscription that
is fresh for local broker, and if there exists a retain message, remote broker would
return the retain message inside PRIVATE RETAIN.
<br><br>
The cluster current also support node/subscription recover, crash detection.
