# Setup local Connect cluster with Confluent Cloud Kafka Cluster

In this document, we will look at the different steps needed to setup a local Connect cluster 
backed by a Kafka cluster in the staging cloud. 

## Setup Kafka Cluster in Cloud

Firstly, setup a Kafka Cluster in Staging Cloud
1. Sign up at the staging cluster (https://stag.cpdev.cloud).
2. Create a Kafka cluster.
3. Copy the configurations they provide into a local text file. My configs look like:

```
bootstrap.servers=SASL_SSL://<broker-list>
ssl.endpoint.identification.algorithm=https
sasl.mechanism=PLAIN
request.timeout.ms=20000
retry.backoff.ms=500
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<api-key-id>" password="<secret-access-key>";
security.protocol=SASL_SSL
```

Replace <api-key-id> and <secret-access-key> with correct values for your cluster.

## Install CCloud

Once we have a working Kafka cluster in the cloud, we need a command line tool to interact with it from 
our laptops. For this, we will use the `ccloud` utility (https://github.com/confluentinc/homebrew-ccloud). 
To install it, use the following commands: 

```
$ brew tap confluentinc/ccloud
$ brew install ccloud
```

Once `ccloud` is installed, we need to configure to work with our cluster. Run `ccloud init` and provide 
the answers to the questions. This is how my session looked like:

```
$ ccloud init
Bootstrap broker list: SASL_SSL://<broker-list>
API Key: <api-key-id>
API Secret: <secret-access-key>
Initialized. Saved config to /Users/arjun/.ccloud/config
```

To test your Kafka cluster and ccloud setup, first create at test topic with one partition:

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
and install in a directory, cd into it and create two files (`staging-connect-standalone.properties` 
and `staging-file-sink.properties`) in the config directory, whose contents look like the following 
(note the security configs with consumer.* prefix):

```
$ cat config/staging-connect-standalone.properties
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
$ cat config/staging-file-sink.properties

name=staging-file-sink
connector.class=org.apache.kafka.connect.file.FileStreamSinkConnector
tasks.max=1
topics=connect-test
file=my_file.txt
```

Now, run the `connect-standalone` script with these two filenames as arguments:

```
$ ./bin/connect-standalone.sh  config/staging-connect-standalone.properties config/staging-file-sink.properties
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
script instead of the standalone one, and creating the `staging-connect-distributed.properties` file. This 
file is very similar to the standalone properties file (with the exception of a `group.id` property)).

The contents of my `staging-connect-distributed.properties` file are:

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
$ ./bin/connect-distributed.sh config/staging-connect-distributed.properties
```

In order to test if the workers came up correctly, we can setup another file sink as follows. 
Create a file `staging-file-sink.json` whose contents are as follows:

```
$ cat staging-file-sink.json
{
  "name": "staging-file-sink",
  "config": {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
    "tasks.max": 3,
    "topics": "connect-test",
    "file": "/Users/arjun/staging_file.txt"
  }
}
```

Post this connector config to the worker using the curl command:

```
$ curl -s -H "Content-Type: application/json" -X POST -d @staging-file-sink.json http://localhost:8083/connectors/ | jq .
```

This should give the following response:

```
{
  "name": "staging-file-sink",
  "config": {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
    "tasks.max": "1",
    "topics": "connect-test",
    "file": "/Users/arjun/staging_file.txt",
    "name": "staging-file-sink"
  },
  "tasks": [],
  "type": null
}
```

Produce some records using ccloud and tail this file to check if the connectors were successfully created.
