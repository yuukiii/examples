# Kafka Streams examples

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

    $ git clone git@github.com:apache/kafka.git
    $ git checkout trunk
    $ ./gradlew clean installAll


# Development

This project uses the standard maven lifecycle and commands such as:

    $ mvn compile # This also generates Java classes from the Avro schemas
    $ mvn test    # But no tests yet!
