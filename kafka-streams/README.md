# Kafka Streams examples

This sub-folder contains code examples that demonstrate how to implement real-time processing applications using Kafka
Streams, which is a new stream processing library included with the [Apache Kafka](http://kafka.apache.org/) open source
project.


# Apache Kafka and Kafka Streams

The Kafka Streams library is a component of the [Apache Kafka](http://kafka.apache.org/) project that is available from Kafka version 0.10.0.0 onwards.

> Note: Until Kafka 0.10.0.0 is officially released (ETA is May 2016) you must manually build Kafka 0.10.0.0.
>
> ```shell
> $ git clone git@github.com:apache/kafka.git && cd kafka
> $ git checkout 0.10.0
> # Perhaps you need to bootstrap `gradlew` first, see Kafka's top-level README:
> #     $ gradle
> $ ./gradlew clean installAll
> ```
>
> Sorry for this temporary inconvenience!

# List of examples

## Java

> Note: We use the label "Lambda" to denote any examples that make use of lambda expressions and thus require Java 8+.

* [WordCountLambdaExample](src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java)
  -- demonstrates, using the Kafka Streams DSL, how to implement the WordCount program that computes a simple word
  occurrence histogram from an input text.
* [PageViewRegionLambdaExample](src/main/java/io/confluent/examples/streams/PageViewRegionLambdaExample.java)
  -- demonstrates how to perform a join between a `KStream` and a `KTable`, i.e. an example of a stateful computation
    * Variant: [PageViewRegionExample](src/main/java/io/confluent/examples/streams/PageViewRegionExample.java),
      which implements the same example but without lambda expressions and thus works with Java 7+.
* [MapFunctionLambdaExample](src/main/java/io/confluent/examples/streams/MapFunctionLambdaExample.java)
  -- demonstrates how to perform simple, state-less transformations via map functions, using the Kafka Streams DSL
  (see also the Scala variant
  [MapFunctionScalaExample](src/main/scala/io/confluent/examples/streams/MapFunctionScalaExample.scala))
* Working with data in Apache Avro format (see also the end-to-end demos under integration tests below):
    * Generic Avro:
      [PageViewRegionLambdaExample](src/main/java/io/confluent/examples/streams/PageViewRegionLambdaExample.java)
      (Java 8+) and
      [PageViewRegionExample](src/main/java/io/confluent/examples/streams/PageViewRegionExample.java) (Java 7+)
    * Specific Avro:
      [WikipediaFeedAvroLambdaExample](src/main/java/io/confluent/examples/streams/WikipediaFeedAvroLambdaExample.java)
      (Java 8+) and
      [WikipediaFeedAvroExample](src/main/java/io/confluent/examples/streams/WikipediaFeedAvroExample.java) (Java 7+)
* And [further examples](src/main/java/io/confluent/examples/streams/).

There are also a few **integration tests**, which demonstrate end-to-end data pipelines.  Here, we spawn embedded Kafka
clusters, feed input data to them, process the data using Kafka Streams, and finally verify the output results.

> Tip: Run `mvn test` to launch the integration tests.

* [WordCountLambdaIntegrationTest](src/test/java/io/confluent/examples/streams/WordCountLambdaIntegrationTest.java)
* [JoinLambdaIntegrationTest](src/test/java/io/confluent/examples/streams/JoinLambdaIntegrationTest.java)
* [MapFunctionLambdaIntegrationTest](src/test/java/io/confluent/examples/streams/MapFunctionLambdaIntegrationTest.java)
* [PassThroughIntegrationTest](src/test/java/io/confluent/examples/streams/PassThroughIntegrationTest.java)
* [GenericAvroIntegrationTest.java](src/test/java/io/confluent/examples/streams/GenericAvroIntegrationTest.java)
* [SpecificAvroIntegrationTest.java](src/test/java/io/confluent/examples/streams/SpecificAvroIntegrationTest.java)


## Scala

* [MapFunctionScalaExample](src/main/scala/io/confluent/examples/streams/MapFunctionScalaExample.scala)
  -- demonstrates how to perform simple, state-less transformations via map functions, using the Kafka Streams DSL
  (see also the Java variant
  [MapFunctionLambdaExample](src/main/java/io/confluent/examples/streams/MapFunctionLambdaExample.java))

There is also an integration test, which demonstrates end-to-end data pipelines.  Here, we spawn embedded Kafka
clusters, feed input data to them, process the data using Kafka Streams, and finally verify the output results.

> Tip: Run `mvn test` to launch the integration tests.

* [JoinScalaIntegrationTest](src/test/scala/io/confluent/examples/streams/JoinScalaIntegrationTest.scala)


# Requirements

## Apache Kafka with Kafka Streams included

The code in this repository requires a version of Apache Kafka that already includes its
[Kafka Streams](https://github.com/apache/kafka/tree/trunk/streams) library.


## Java 8

Some code examples require Java 8, primarily because of the usage of
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


# Packaging and running the examples

> Tip:  You can also run `mvn test`, which executes the included integration tests.  These tests spawn embedded Kafka
> clusters to showcase the Kafka Streams functionality end-to-end.  The benefit of the integration tests is that you
> don't need to install and run a Kafka cluster yourself.

If you want to run the examples against a Kafka cluster, you may want to create a standalone jar ("fat jar") of the
Kafka Streams examples via:

```shell
# Create a standalone jar
$ mvn clean package

# >>> Creates target/streams-examples-3.0.0-SNAPSHOT-standalone.jar
```

You can now run the example applications as follows:

```shell
# Run an example application from the standalone jar.
# Here: `WordCountLambdaExample`
$ java -cp target/streams-examples-3.0.0-SNAPSHOT-standalone.jar \
  io.confluent.examples.streams.WordCountLambdaExample
```

Keep in mind that the machine on which you run the command above must have access to the Kafka/ZK clusters you
configured in the code examples.  By default, the code examples assume the Kafka cluster is accessible via
`localhost:9092` (Kafka broker) and the ZooKeeper ensemble via `localhost:2181`.


# Development

This project uses the standard maven lifecycle and commands such as:

```shell
$ mvn compile # This also generates Java classes from the Avro schemas
$ mvn test    # But no tests yet!
```
