# Kafka Streams examples

This sub-folder contains code examples that demonstrate how to implement real-time processing applications using Kafka
Streams, which is a new stream processing library included with the [Apache Kafka](http://kafka.apache.org/) open source
project.


# Apache Kafka and Kafka Streams

The Kafka Streams library is a component of the [Apache Kafka](http://kafka.apache.org/) project.

As of March 2016 the Apache Kafka project does not yet provide an official release that includes the new Kafka Streams
library (Kafka Streams is expected to be released with upcoming Kafka 0.10).  For this reason the code examples in this
directory depend on the [Confluent Platform 2.1.0-alpha1 release](http://www.confluent.io/developer) aka the
_Kafka Streams Tech Preview_, which includes the latest Kafka Streams implementation backported to Kafka 0.9.


# Requirements

## Java 8

The code examples require Java 8, primarily because of the usage of
[lambda expressions](https://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html).

IntelliJ IDEA users:

* Open _File > Project structure_
* Select "Project" on the left.
    * Set "Project SDK" to Java 1.8.
    * Set "Project language level" to "8 - Lambdas, type annotations, etc."


## A Kafka version that includes Kafka Streams

You must first build the latest version of Kafka `trunk` and install it locally:

```shell
$ git clone git@github.com:apache/kafka.git
$ git checkout trunk
$ ./gradlew clean
$ ./gradlew -PscalaVersion=2.11.7 install
```


# Development

This project uses the standard maven lifecycle and commands such as:

```shell
$ mvn compile # This also generates Java classes from the Avro schemas
$ mvn test    # But no tests yet!
```
