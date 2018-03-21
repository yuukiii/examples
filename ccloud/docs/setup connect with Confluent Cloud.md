# Setup local Connect cluster with Confluent Cloud Kafka Cluster

## Setup Kafka Cluster in Cloud

Follow the documentation https://docs.confluent.io/current/cloud-quickstart.html to
- Log into the Confluent Cloud web interface and create a Kafka Cluster
- Install and configure Confluent Cloud CLI
- Create Topics and Produce and Consume to Kafka

## Create an example `connect-test` topic

To test your Kafka cluster and ccloud setup, first create a test topic with one partition:

```
$ ccloud topic create --partitions 1 connect-test
```

then, produce some records using ccloud (press Ctrl+D to end producing):

```
$ ccloud produce --topic connect-test
{"field1": "hello", "field2": 1}
{"field1": "hello", "field2": 2}
{"field1": "hello", "field2": 3}
{"field1": "hello", "field2": 4}
{"field1": "hello", "field2": 5}
{"field1": "hello", "field2": 6}
^D
```

and check if they can be consumed: 

```
$ ccloud consume -b --topic connect-test
{"field1": "hello", "field2": 1}
{"field1": "hello", "field2": 2}
{"field1": "hello", "field2": 3}
{"field1": "hello", "field2": 4}
{"field1": "hello", "field2": 5}
{"field1": "hello", "field2": 6}
```

## Setting Up a Standalone Connect Worker with this Cloud Cluster

Download the latest distribution of Apache Kafka from https://kafka.apache.org/downloads. Unzip 
and install in a directory, cd into it and create two files (`example-connect-standalone.properties` 
and `example-file-sink.properties`) in the config directory, whose contents look like the following 
(note the security configs with consumer.* prefix):

```
$ cat config/example-connect-standalone.properties
bootstrap.servers=<broker-list>

# The converters specify the format of data in Kafka and how to translate it into Connect data. Every Connect user will
# need to configure these based on the format they want their data in when loaded from or stored into Kafka
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
# Converter-specific settings can be passed in by prefixing the Converter's setting with the converter we want to apply
# it to
key.converter.schemas.enable=false
value.converter.schemas.enable=false

# The internal converter used for offsets and config data is configurable and must be specified, but most users will
# always want to use the built-in default. Offset and config data is never visible outside of Kafka Connect in this format.
internal.key.converter=org.apache.kafka.connect.json.JsonConverter
internal.value.converter=org.apache.kafka.connect.json.JsonConverter
internal.key.converter.schemas.enable=false
internal.value.converter.schemas.enable=false

offset.storage.file.filename=/tmp/connect.offsets
# Flush much faster than normal, which is useful for testing/debugging
offset.flush.interval.ms=10000

ssl.endpoint.identification.algorithm=https
sasl.mechanism=PLAIN
request.timeout.ms=20000
retry.backoff.ms=500
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<api-key-id>" password="<secret-access-key>";
security.protocol=SASL_SSL

consumer.ssl.endpoint.identification.algorithm=https
consumer.sasl.mechanism=PLAIN
consumer.request.timeout.ms=20000
consumer.retry.backoff.ms=500
consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<api-key-id>" password="<secret-access-key>";
consumer.security.protocol=SASL_SSL
```

and: 

```
$ cat config/example-file-sink.properties

name=example-file-sink
connector.class=org.apache.kafka.connect.file.FileStreamSinkConnector
tasks.max=1
topics=connect-test
file=my_file.txt
```

Now, run the `connect-standalone` script with these two filenames as arguments:

```
$ ./bin/connect-standalone.sh  config/example-connect-standalone.properties config/example-file-sink.properties
```

This should start a connect worker on your machine which will consume the records we produced earlier 
using the ccloud command. If you tail the contents of `my_file.txt`, it should be the following:

```
$ tail -f my_file.txt
{"field1": "hello", "field2": 1}
{"field1": "hello", "field2": 2}
{"field1": "hello", "field2": 3}
{"field1": "hello", "field2": 4}
{"field1": "hello", "field2": 5}
{"field1": "hello", "field2": 6}
```

## Setting Up a Distributed Connect Worker with this Cloud Cluster

The process here is exactly same as above. The only difference being to run the `connect-distributed.sh` 
script instead of the standalone one, and creating the `example-connect-distributed.properties` file. This 
file is very similar to the standalone properties file (with the exception of a `group.id` property)).

The contents of my `example-connect-distributed.properties` file are:

```
bootstrap.servers=<broker-list>

group.id=connect-cluster

key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false

internal.key.converter=org.apache.kafka.connect.json.JsonConverter
internal.value.converter=org.apache.kafka.connect.json.JsonConverter
internal.key.converter.schemas.enable=false
internal.value.converter.schemas.enable=false

offset.storage.topic=connect-offsets
offset.storage.replication.factor=3

config.storage.topic=connect-configs
config.storage.replication.factor=3

status.storage.topic=connect-status
status.storage.replication.factor=3

offset.flush.interval.ms=10000

ssl.endpoint.identification.algorithm=https
sasl.mechanism=PLAIN
request.timeout.ms=20000
retry.backoff.ms=500
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<api-key-id>" password="<secret-access-key>";
security.protocol=SASL_SSL

consumer.ssl.endpoint.identification.algorithm=https
consumer.sasl.mechanism=PLAIN
consumer.request.timeout.ms=20000
consumer.retry.backoff.ms=500
consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<api-key-id>" password="<secret-access-key>";
consumer.security.protocol=SASL_SSL
```

Run Connect using the following command:

```
$ ./bin/connect-distributed.sh config/example-connect-distributed.properties
```

In order to test if the workers came up correctly, we can setup another file sink as follows. 
Create a file `example-file-sink.json` whose contents are as follows:

```
$ cat example-file-sink.json
{
  "name": "example-file-sink",
  "config": {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
    "tasks.max": 3,
    "topics": "connect-test",
    "file": "/Users/<user-name>/example_file.txt"
  }
}
```

Post this connector config to the worker using the curl command:

```
$ curl -s -H "Content-Type: application/json" -X POST -d @example-file-sink.json http://localhost:8083/connectors/ | jq .
```

This should give the following response:

```
{
  "name": "example-file-sink",
  "config": {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
    "tasks.max": "1",
    "topics": "connect-test",
    "file": "/Users/<user-name>/example_file.txt",
    "name": "example-file-sink"
  },
  "tasks": [],
  "type": null
}
```

Produce some records using ccloud and tail this file to check if the connectors were successfully created.
