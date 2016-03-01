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


# List of examples

Java:

* [MapFunctionExample.java](src/main/java/io/confluent/examples/streams/MapFunctionExample.java)
  -- demonstrates how to perform simple, state-less transformations via map functions, using the high-level KStream DSL

Scala:

* [MapFunctionScalaExample](src/main/scala/io/confluent/examples/streams/MapFunctionScalaExample.scala)
  -- demonstrates how to perform simple, state-less transformations via map functions, using the high-level KStream DSL

There are also a few integration tests, which demonstrate end-to-end data pipelines.  Here, we spawn embedded Kafka
clusters, feed input data to them, process the data using Kafka Streams, and finally verify the output results.

* [WordCountIntegrationTest](src/test/java/io/confluent/examples/streams/WordCountIntegrationTest.java)
* [MapFunctionIntegrationTest](src/test/java/io/confluent/examples/streams/MapFunctionIntegrationTest.java)
* [PassThroughIntegrationTest](src/test/java/io/confluent/examples/streams/PassThroughIntegrationTest.java)

> Tip: Run `mvn test` to launch the integration tests.


# Requirements

## Java 8

The code examples require Java 8, primarily because of the usage of
[lambda expressions](https://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html).

IntelliJ IDEA users:

* Open _File > Project structure_
* Select "Project" on the left.
    * Set "Project SDK" to Java 1.8.
    * Set "Project language level" to "8 - Lambdas, type annotations, etc."


## Scala

> Scala is required only for the Scala examples in this repository.  If you are a Java developer you can safely ignore
> this section.

If you want to experiment with the Scala examples in this repository, you need a version of Scala that supports Java 8
and SAM / Java lambda (e.g. Scala 2.11 with * `-Xexperimental` compiler flag, or 2.12).


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
