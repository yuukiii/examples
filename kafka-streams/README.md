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

## Java

* [WordCountLambdaExample.java](src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java)
  -- demonstrates, using the high-level KStream DSL, how to implement the WordCount program that computes a simple word
  occurrence histogram from an input text.
    * Variant 1: [WordCountAvroLambdaExample.java](src/main/java/io/confluent/examples/streams/WordCountAvroLambdaExample.java),
      which implements a similar WordCount-like algorithm but demonstrates how to process data in Apache Avro format
    * Variant 2: [WordCountAvroExample.java](src/main/java/io/confluent/examples/streams/WordCountAvroExample.java)
      -- same as variant 1 but does not use lambda expressions, which means you can run this code on Java 7+.
* [MapFunctionLambdaExample.java](src/main/java/io/confluent/examples/streams/MapFunctionLambdaExample.java)
  -- demonstrates how to perform simple, state-less transformations via map functions, using the high-level KStream DSL
* [PageViewRegionLambdaExample.java](src/main/java/io/confluent/examples/streams/PageViewRegionLambdaExample.java)
  -- computes the number of page views

There are also a few integration tests, which demonstrate end-to-end data pipelines.  Here, we spawn embedded Kafka
clusters, feed input data to them, process the data using Kafka Streams, and finally verify the output results.

> Tip: Run `mvn test` to launch the integration tests.

* [WordCountLambdaIntegrationTest](src/test/java/io/confluent/examples/streams/WordCountLambdaIntegrationTest.java)
* [MapFunctionLambdaIntegrationTest](src/test/java/io/confluent/examples/streams/MapFunctionLambdaIntegrationTest.java)
* [PassThroughIntegrationTest](src/test/java/io/confluent/examples/streams/PassThroughIntegrationTest.java)


## Scala

* [MapFunctionScalaExample](src/main/scala/io/confluent/examples/streams/MapFunctionScalaExample.scala)
  -- demonstrates how to perform simple, state-less transformations via map functions, using the high-level KStream DSL


# Requirements

## Apache Kafka with Kafka Streams included

> The instructions in this section are only required until the Kafka Streams Tech Preview is released.

Until the Kafka Streams Tech Preview is available, you have three options to get your hands on the Kafka Streams
library, sorted by ease-of-use (easiest first):

1. Use Confluent's staging maven repository by adding/editing the following snippet to your copy of [pom.xml](pom.xml):

    ```xml
    <repositories>
        <!-- Confluent's production maven repository is already defined in `pom.xml` -->
        <repository>
            <id>confluent</id>
            <url>http://packages.confluent.io/maven/</url>
        </repository>
        <!-- This is Confluent's staging maven repository; add this when/where needed -->
        <repository>
            <id>confluent-staging</id>
            <url>http://staging-confluent-packages-maven-2.1.0.s3.amazonaws.com/maven/</url>
        </repository>
    </repositories>
    ```

2. Build Apache Kafka 0.9 with backported Kafka Streams, and install it locally.  No changes required to
   [pom.xml](pom.xml).

    ```shell
    $ git clone git@github.com:confluentinc/kafka.git        # Confluent mirror of Apache Kafka's git repository
    $ git checkout 0.9.0-cp-2.0.1-with-streams-tech-preview  # Kafka 0.9 with backported Kafka Streams
    $ ./gradlew clean installAll
    ```

3. Build the latest version of Apache Kafka `trunk`, and install it locally.  Note that, in this case, you will need to
   update [pom.xml](pom.xml) to match the specified version of Kafka with the one in Kafka's `trunk` (cf.
   `version` in [gradle.properties](https://github.com/apache/kafka/blob/trunk/gradle.properties));  as of 01-Mar-2016,
   this version is `0.10.0.0-SNAPSHOT`.

    ```shell
    $ git clone git@github.com:apache/kafka.git
    $ git checkout trunk
    $ ./gradlew clean installAll
    ```

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


# Packaging and running the examples

> Tip:  You can also run `mvn test`, which executes the included integration tests.  These tests spawn embedded Kafka
> clusters to showcase the Kafka Streams functionality end-to-end.  The benefit of the integration tests is that you
> don't need to install and run a Kafka cluster yourself.

If you want to run the examples against a Kafka cluster, you may want to create a standalone jar ("fat jar") of the
Kafka Streams examples via:

```shell
# Create a standalone jar
$ mvn clean package

# >>> Creates target/streams-examples-2.1.0-alpha1-standalone.jar
```

You can now run the example applications as follows:

```shell
# Run an example application from the standalone jar.
# Here: `WordCountLambdaExample`
$ java -cp target/streams-examples-2.1.0-alpha1-standalone.jar \
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
