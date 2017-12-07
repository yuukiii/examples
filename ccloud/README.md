# Kafka client examples

This directory includes projects demonstrating how to use the Java Kafka producer
and consumer. You can find detailed explanation of the code at the
[application development section](http://docs.confluent.io/3.2.2/app-development.html)
of the Confluent Platform documentation.


# Requirements


* JDK Version 1.8.0_102 or greater.


# Setup ccloud CLI

## Installation

### Mac OS X

```shell
$ brew tap confluentinc/ccloud
$ brew install ccloud
```

### Linux

Download and unzip from [here](https://s3-us-west-2.amazonaws.com/confluent.cloud/cli/ccloud-latest.tar.gz).  Add the contents of the bin directory to your PATH environment variable so that `which ccloud` finds the ccloud command.
	

### Windows

Download and unzip from [here](https://s3-us-west-2.amazonaws.com/confluent.cloud/cli/ccloud-latest.zip).  Use the bin/ccloud.ps1 Powershell script to run ccloud CLI.

## Setup

To get started you will need to configure ccloud by running the `init` command.

```shell
$ ccloud init
Bootstrap broker list: <broker_list>
API Key: <key>
API Secret: <secret>
Initialized. Saved config to ~/.ccloud/config
```

Now that you have everything configured, let's take a look at the usage message for ccloud.

```shell
$ ccloud help
usage: ccloud [(--verbose | -v)] [(--config-dir <configDir> | -c <configDir>)]
        <command> [<args>]

The most commonly used ccloud commands are:
    consume   Consume from a topic.
    help      Display help information
    init      Initialize the CLI.
    produce   Produce to a topic.
    topic     Manage topics.
    version   Print the version number.

See 'ccloud help <command>' for more information on a specific command.
```

Let's go ahead and retrieve the list of topics in your Confluent Cloud cluster.

```shell
$ ccloud topic list
No topics found !
```

Since this is a new cluster there are no topics configured. Let's change that now.

To create a topic, use the `topic` command. To start, let's get some help.

```shell
$ ccloud help topic create
NAME
        ccloud topic create - Create a topic.

SYNOPSIS
        ccloud [(--config-dir <configDir> | -c <configDir>)] [(--verbose | -v)]
                topic create [--config <config>] [--partitions <partitions>]
                [--replication-factor <replicationFactor>] [--] <topicName>

OPTIONS
        --config <config>
            A comma separated listed of topic configuration (key=value)
            overrides for the topic being altered.

        --config-dir <configDir>, -c <configDir>
            Path to configuration dir. It should contain a file called config.
            Default: $HOME/.ccloud

        --partitions <partitions>
            Number of topic partitions.

        --replication-factor <replicationFactor>
            Replication factor.

        --verbose, -v
            Make the world verbose.

        --
            This option can be used to separate command-line options from the
            list of argument, (useful when arguments might be mistaken for
            command-line options

        <topicName>
            Name of topic.
```

Given that information, let's go ahead and create a `test` topic.

```
$ ccloud topic create test
Topic "test" created.
```

To get all of the configuration details about the test topic let's use the `topic describe` command.

```
$ ccloud topic describe test
Topic:test	PartitionCount:12	ReplicationFactor:3	Configs:min.insync.replicas=2
	Topic: test	Partition: 0	Leader: 16	Replicas: 16,12,13	Isr: 16,12,13
	Topic: test	Partition: 1	Leader: 17	Replicas: 17,13,14	Isr: 17,13,14
	Topic: test	Partition: 2	Leader: 0	Replicas: 0,14,15	Isr: 0,14,15
	Topic: test	Partition: 3	Leader: 1	Replicas: 1,15,16	Isr: 1,15,16
	Topic: test	Partition: 4	Leader: 2	Replicas: 2,16,17	Isr: 2,16,17
	Topic: test	Partition: 5	Leader: 3	Replicas: 3,17,0	Isr: 3,17,0
	Topic: test	Partition: 6	Leader: 4	Replicas: 4,0,1	Isr: 4,0,1
	Topic: test	Partition: 7	Leader: 5	Replicas: 5,1,2	Isr: 5,1,2
	Topic: test	Partition: 8	Leader: 6	Replicas: 6,2,3	Isr: 6,2,3
	Topic: test	Partition: 9	Leader: 7	Replicas: 7,3,4	Isr: 7,3,4
	Topic: test	Partition: 10	Leader: 8	Replicas: 8,4,5	Isr: 8,4,5
	Topic: test	Partition: 11	Leader: 9	Replicas: 9,5,6	Isr: 9,5,6
```

When creating topics, the most likely per-topic configs you'll want to change are: partition count and retention period. Here is an example of how to set them both when creating a new topic and then checking the configuration using `topic describe`.

```
$ ccloud topic create testCustom --partitions 20 --config retention.ms=259200000
Topic "testCustom" created.

$ ccloud topic describe testCustom
Topic:testCustom	PartitionCount:20	ReplicationFactor:3	Configs:min.insync.replicas=2,retention.ms=259200000
	Topic: testCustom	Partition: 0	Leader: 1	Replicas: 1,8,9	Isr: 1,8,9
	Topic: testCustom	Partition: 1	Leader: 2	Replicas: 2,9,10	Isr: 2,9,10
	Topic: testCustom	Partition: 2	Leader: 3	Replicas: 3,10,11	Isr: 3,10,11
	Topic: testCustom	Partition: 3	Leader: 4	Replicas: 4,11,12	Isr: 4,11,12
	Topic: testCustom	Partition: 4	Leader: 5	Replicas: 5,12,13	Isr: 5,12,13
	Topic: testCustom	Partition: 5	Leader: 6	Replicas: 6,13,14	Isr: 6,13,14
	Topic: testCustom	Partition: 6	Leader: 7	Replicas: 7,14,15	Isr: 7,14,15
	Topic: testCustom	Partition: 7	Leader: 8	Replicas: 8,15,16	Isr: 8,15,16
	Topic: testCustom	Partition: 8	Leader: 9	Replicas: 9,16,17	Isr: 9,16,17
	Topic: testCustom	Partition: 9	Leader: 10	Replicas: 10,17,0	Isr: 10,17,0
	Topic: testCustom	Partition: 10	Leader: 11	Replicas: 11,0,1	Isr: 11,0,1
	Topic: testCustom	Partition: 11	Leader: 12	Replicas: 12,1,2	Isr: 12,1,2
	Topic: testCustom	Partition: 12	Leader: 13	Replicas: 13,2,3	Isr: 13,2,3
	Topic: testCustom	Partition: 13	Leader: 14	Replicas: 14,3,4	Isr: 14,3,4
	Topic: testCustom	Partition: 14	Leader: 15	Replicas: 15,4,5	Isr: 15,4,5
	Topic: testCustom	Partition: 15	Leader: 16	Replicas: 16,5,6	Isr: 16,5,6
	Topic: testCustom	Partition: 16	Leader: 17	Replicas: 17,6,7	Isr: 17,6,7
	Topic: testCustom	Partition: 17	Leader: 0	Replicas: 0,7,8	Isr: 0,7,8
	Topic: testCustom	Partition: 18	Leader: 1	Replicas: 1,9,10	Isr: 1,9,10
	Topic: testCustom	Partition: 19	Leader: 2	Replicas: 2,10,11	Isr: 2,10,11
```

Now let's produce some messages into the `test` topic.

```
ccloud produce -t test
foo
bar
baz
^D
```

Let's see what we consume from the same topic.

```
$ ccloud consume -b -t test
bar
baz
foo
^C
Processed a total of 3 messages.
```

Note that the order the messages are consumed does not match the order in which they were produced. This is because the producer spread them over the 12 partitions in the `test` topic and consumer reads from all 12 partitions in parallel.

If you hit a snag please use the -v option. This will give you lots of information about what is happening under the covers which can be useful when debugging.

# Quickstart

1. Create a topic called `page_visits`:

	```shell
	# Create page_visits topic
	$ ccloud topic create page_visits
	```
	You should see
	
	```
	Topic "page_visits" created.
	```
	Make sure the topic is created successfully.
	
	```shell
	$ ccloud topic describe page_visits
	```
	
	You should see

	```
	Topic:page_visits	PartitionCount:1	ReplicationFactor:3	Configs:min.insync.replicas=2
	Topic: page_visits	Partition: 0	Leader: 0	Replicas: 0,1,2	Isr: 0,1,2
	Topic: page_visits	Partition: 1	Leader: 1	Replicas: 1,2,0	Isr: 1,2,0
	Topic: page_visits	Partition: 2	Leader: 2	Replicas: 2,0,1	Isr: 2,0,1
	Topic: page_visits	Partition: 3	Leader: 0	Replicas: 0,2,1	Isr: 0,2,1
	Topic: page_visits	Partition: 4	Leader: 1	Replicas: 1,0,2	Isr: 1,0,2
	Topic: page_visits	Partition: 5	Leader: 2	Replicas: 2,1,0	Isr: 2,1,0
	Topic: page_visits	Partition: 6	Leader: 0	Replicas: 0,1,2	Isr: 0,1,2
	Topic: page_visits	Partition: 7	Leader: 1	Replicas: 1,2,0	Isr: 1,2,0
	Topic: page_visits	Partition: 8	Leader: 2	Replicas: 2,0,1	Isr: 2,0,1
	Topic: page_visits	Partition: 9	Leader: 0	Replicas: 0,2,1	Isr: 0,2,1
	Topic: page_visits	Partition: 10	Leader: 1	Replicas: 1,0,2	Isr: 1,0,2
	Topic: page_visits	Partition: 11	Leader: 2	Replicas: 2,1,0	Isr: 2,1,0
	```

1. Now we can turn our attention to the client examples in this directory.

	1. First run the example producer in the [java-clients](java-clients) sub-folder to publish 10 data records to Kafka.

		```shell
		$ cd java-clients
		
		# Build the client examples
		$ mvn clean package
		
		# Run the producer
		$ mvn exec:java -Dexec.mainClass="io.confluent.examples.clients.ProducerExample" \
		  -Dexec.args="$HOME/.ccloud/config page_visits 10"
		```
		You should see
		
		```
		<snipped>
		
		[2017-07-27 14:35:27,944] INFO Kafka version : 0.10.2.1-cp2 (org.apache.kafka.common.utils.AppInfoParser)
		[2017-07-27 14:35:27,944] INFO Kafka commitId : 8041e4a077aba712 (org.apache.kafka.common.utils.AppInfoParser)
		Successfully produced 10 messages to page_visits.
		....
		```
		

	1. Then, run the Kafka consumer application in the [java-clients](java-clients) sub-folder to read the records we just published to the Kafka cluster, and to display the records in the console.

		```shell
		$ cd java-clients
		
		# Build the client examples
		$ mvn clean package
		
		# Run the consumer
		$ mvn exec:java -Dexec.mainClass="io.confluent.examples.clients.ConsumerExample" \
		  -Dexec.args="$HOME/.ccloud/config page_visits"		
		```
		
		You should see

		```
		[INFO] Scanning for projects...
		
		<snipped>
		
		[2017-07-27 14:35:41,726] INFO Setting newly assigned partitions [page_visits-11, page_visits-10, page_visits-1, page_visits-0, page_visits-7, page_visits-6, page_visits-9, page_visits-8, page_visits-3, page_visits-2, page_visits-5, page_visits-4] for group example-1759082952 (org.apache.kafka.clients.consumer.internals.ConsumerCoordinator)
		offset = 3, key = 192.168.2.13, value = 1501191328318,www.example.com,192.168.2.13
		offset = 3, key = 192.168.2.225, value = 1501191328318,www.example.com,192.168.2.225
		....
		```
		Hit Ctrl+C to stop.
