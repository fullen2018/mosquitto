Mosquitto with cluster
=================

Mosquitto with cluster implement a non-centralized cluster.
The cluster is a full autonomy mosquitto cluster without any leader or key node,
to make the system with a high availability.

## Usage

Install mosquitto on all of the nodes and write the addresses into mosquitto.conf, e.g.,<br>
node_name node1<br>
node_address 192.168.1.1:1883<br>
<br>
node_name node2<br>
node_address 192.168.1.2:1883<br>

Then config the loadbalancer, take above adresses as real server address. It is strongly recommend to terminate TLS on the loadbalancer, and use raw TCP inside the cluster.<br>

## Installing

Inside config.mk, comment "WITH_BRIDGE:=yes", and uncomment "WITH_CLUSTER:=yes".<br>

See <http://mosquitto.org/download/> for details on installing binaries for
various platforms.

## Cluster Specification

Broadcast clients' subscription and unsubscription to each other brokers inside the cluster.<br>
### Traffic cycle avoid
In order to avoid infinite PUB/SUB forwarding, the publishes from other brokers will only send to client, the subscription and unsubscription from other brokers will not be forward.<br>
### Duplicate subscription avoid
Save local clients' subscriptions and unsubscriptions by a reference counter, only forward the subscription which is fresh for local broker, and only forward the unsubscription which is no longger been subscribed by local client.<br>
### Private messages
Some private messages was introduced in order to support cluster session and retain message, i.e.,<br>
PRIVATE SUBSCRIBE<br>
Fix header|PacketID|Topic Filter|QoS|ClientID|SubID<br><br>
PRIVATE RETAIN<br>
Fix header|Topic|QoS|[PacketID]|ClientID|SubID|OriginalRecvTime|Payload<br><br>
SESSION REQ<br>
Fix header|ClientID|Clean Session<br><br>
SESSION RESP<br>
Fix header|ClientID|PacketID|NRsubs|sub1(topic|qos)|...|subN|NRpubs|pub1(topic|state|dir|dup|qos|mid|payload)|...|pubN|<br>
### Cluster session support
For cluster session, SESSION REQ would be broadcast for each client CONNECT which has no previous context found in local broker, remote broker which has this client's context will kick-off this client and if clean session set to false, the remote broker would return this client's session include subscription, incomplete publishes with QoS>0 inside SESSION RESP. This feature could be disable in mosquitto.conf, in order to save inside traffic.<br>
### Cluster retain message support
For retain message, PRIVATE SUBSCRIBE would be broadcast for each client subscription that
is fresh for local broker, and if there exists a retain message, remote broker would
return the retain message inside PRIVATE RETAIN. Client will receive the most recent retain message inside the cluster after a frozen window which can be configure inside mosquitto.conf.<br>
### QoS support
For QoS, all the publishes set with it's original QoS inside the cluster, and process with QoS=0, to saves the inside traffic. The actual QoS send to client decide by the broker which client connected with.<br>
### Other features
All validation, utf-8, ACL checking has disabled for the messages inside the cluster, in order to improve cluster efficiency.<br><br>
Once CONNECT success with other broker, local broker will send all the local clients' subscription to remote broker, to avoid subscription loss while some brokers down and up.<br><br>
The cluster current also support node/subscription recover, crash detection.
