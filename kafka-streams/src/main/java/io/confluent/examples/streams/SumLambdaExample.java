/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.confluent.examples.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

/**
 * Demonstrates how to use `reduceByKey` to sum numbers.
 *
 * See `SumLambdaIntegrationTest` for an end-to-end example.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class SumLambdaExample {

  public static void main(String[] args) throws Exception {
    Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-lambda-example");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // Where to find the corresponding ZooKeeper ensemble.
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());

    KStreamBuilder builder = new KStreamBuilder();
    // We assume the input topic contains records where the values are Integers.
    // We don't really care about the keys of the input records;  for simplicity, we assume them
    // to be Integers, too, because we will re-key the stream later on, and the new key will be
    // of type Integer.
    KStream<Integer, Integer> input = builder.stream("numbers-topic");
    KTable<Integer, Integer> sumOfOddNumbers = input
        // We are only interested in odd numbers.
        .filter((k, v) -> v % 2 == 0)
        // We want to compute the total sum across ALL numbers, so we must re-key all records to the
        // same key.  This re-keying is required because in Kafka Streams a data record is always a
        // key-value pair, and KStream aggregations such as `reduceByKey` operate on a per-key basis.
        // The actual new key (here: `1`) we pick here doesn't matter as long it is the same across
        // all records.
        .selectKey((k, v) -> 1)
        // Add the numbers to compute the sum.
        .reduceByKey((v1, v2) -> v1 + v2, "sum");
    sumOfOddNumbers.to("sum-of-odd-numbers-topic");

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();
  }

}