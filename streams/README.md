# Kafka Streams examples

This sub-repository includes examples demonstrating how to implement real-time processing applications using Kafka Streams.

It builds based on the Confluent Platform 2.1.0-alpha1 (aka the "Streams Tech Preview") release.

# Requirements

## Java 8

The code examples require Java 8.

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
