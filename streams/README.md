# Kafka Streams examples

# Requirements

## Prerequisites

* Java 8

Make sure your IDE is configured to use Java 8, too.

IntelliJ IDEA users:

* Open _File > Project structure_
* Select "Project" on the left.
    * Set "Project SDK" to Java 1.8.
    * Set "Project language level" to "8 - Lambdas, type annotations, etc."


## Kafka version that includes Kafka Streams

You must first build the latest version of Kafka `trunk` and install it locally:

    $ git clone git@github.com:apache/kafka.git
    $ git checkout trunk
    $ ./gradlew clean installAll
