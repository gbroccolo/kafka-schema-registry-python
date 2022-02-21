# An example of AVRO messages exchanged between Python producers and consumers using the Confluent schema registry

The repository contains the `Dockerfile` used to dockerise the producer/subscriber
nodes, in addition to the `docker-compose` configuration to orchestrate the build
of the following cluster:

* a `zookeeper` node used to configure and as a veto for the Kafka cluster (in case
of replicas enabled)
* a `kafka-broker` node
* a `schema-registry` node to store the AVRO schemas in the cluster
* a `subscriber` node, implementing the definitions in `src/`
* a `producer` node, implementing the definitions in `src/`

**NOTE**: the subscriber node is always able to get the latest version of the schema
from the registry node

## Schema of the repository

* `avro/` - folder with the AVRO schema used to serialise/deserialise the message
* `src/` - the Python code used to implement the producer and the subscriber

## How to run the example

* start up the cluster
```
$ docker-compose up -d
```
* see topic/partition/offset of published, serialised messages:
```
$ docker logs producer
Message successfully produced to messages[0] at offset 186
Message successfully produced to messages[0] at offset 187
Message successfully produced to messages[0] at offset 188
Message successfully produced to messages[0] at offset 189
Message successfully produced to messages[0] at offset 190
Message successfully produced to messages[0] at offset 191
```

* see consumed, deserialised messages
```
Consumed record 4e03aa9f-be1c-42a6-8488-64d0ee2e0fd3:	string: foo, number: 42
Consumed record 69ca9936-dd1c-40f4-b7b6-f3fe9ff01341:	string: foo, number: 42
Consumed record 6f7a1685-5667-46c0-8d54-7bc8bd9a7947:	string: foo, number: 42
Consumed record 1148b141-285c-410d-bd4e-336f62e779ac:	string: foo, number: 42
Consumed record f6854d31-985b-479d-912c-564967148204:	string: foo, number: 42
Consumed record c3c93df7-dae0-4d34-abe0-c9b4eb3d65e7:	string: foo, number: 42
```

## sending requests to the registry

Schemas, versions, etc. of messages related to the topic can be retrieved via
HTTP from the schema registry:

```
$ curl -X GET http://localhost:8081/subjects
["messages-key","messages-value"]

$ curl -X GET http://localhost:8081/subjects/messages-value/versions
[1]

$ curl -X GET http://localhost:8081/subjects/messages-value/versions/1
{"subject":"messages-value","version":1,"id":1,"schema":"{\"type\":\"record\",\"name\":\"Name\",\"namespace\":\"io.confluent.schema.messages\",\"fields\":[{\"name\":\"stringKey\",\"type\":\"string\"},{\"name\":\"intKey\",\"type\":\"int\"}]}"}
```
