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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test based on WordCountLambdaExample, using an embedded Kafka cluster.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class WordCountLambdaIntegrationTest {

  private static EmbeddedSingleNodeKafkaCluster cluster = null;
  private static final String inputTopic = "inputTopic";
  private static final String outputTopic = "outputTopic";

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    cluster = new EmbeddedSingleNodeKafkaCluster();
    cluster.createTopic(inputTopic);
    cluster.createTopic(outputTopic);
  }

  @AfterClass
  public static void stopKafkaCluster() throws IOException {
    if (cluster != null) {
      cluster.stop();
    }
  }

  @Test
  public void shouldCountWords() throws Exception {
    List<String> inputValues = Arrays.asList("hello", "world", "world", "hello world");
    List<KeyValue<String, Long>> expectedWordCounts = Arrays.asList(
        new KeyValue<>("hello", 1L),
        new KeyValue<>("world", 1L),
        new KeyValue<>("world", 2L),
        new KeyValue<>("hello", 2L),
        new KeyValue<>("world", 3L)
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    // This code is identical to what's shown in WordCountLambdaExample, with the exception of
    // the call to `purgeLocalStreamsState()`, which we need only for cleaning up previous runs of
    // this integration test.
    //
    final Serializer<String> stringSerializer = new StringSerializer();
    final Deserializer<String> stringDeserializer = new StringDeserializer();
    final Serializer<Long> longSerializer = new LongSerializer();
    final Deserializer<Long> longDeserializer = new LongDeserializer();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.JOB_ID_CONFIG, "wordcount-lambda-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, cluster.zookeeperConnect());
    streamsConfiguration.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    streamsConfiguration.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    streamsConfiguration.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    streamsConfiguration.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    // Explicitly place the state directory under /tmp so that we can remove it via
    // `purgeLocalStreamsState` below.  Once Streams is updated to expose the effective
    // StreamsConfig configuration (so we can retrieve whatever state directory Streams came up
    // with automatically) we don't need to set this anymore and can update `purgeLocalStreamsState`
    // accordingly.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, String> textLines = builder.stream(inputTopic);

    KStream<String, Long> wordCounts = textLines
        .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
        .map((key, value) -> new KeyValue<>(value, value))
        .countByKey(stringSerializer, longSerializer, stringDeserializer, longDeserializer, "Counts")
        .toStream();

    wordCounts.to(outputTopic, stringSerializer, longSerializer);

    // Remove any state from previous test runs
    IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();

    // Wait briefly for the topology to be fully up and running (otherwise it might miss some or all
    // of the input data we produce below).
    // Note: The sleep times are relatively high to support running the build on Travis CI.
    Thread.sleep(5000);

    //
    // Step 2: Produce some input data to the input topic.
    //
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    Producer<String, String> producer = new KafkaProducer<>(producerConfig);
    for (String value : inputValues) {
      Future<RecordMetadata> f = producer.send(new ProducerRecord<>(inputTopic, value));
      f.get();
    }
    producer.flush();
    producer.close();

    // Give the stream processing application some time to do its work.
    // Note: The sleep times are relatively high to support running the build on Travis CI.
    Thread.sleep(10000);
    streams.close();

    //
    // Step 3: Verify the application's output data.
    //
    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "wordcount-lambda-integration-test-standard-consumer");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

    KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerConfig);
    consumer.subscribe(Collections.singletonList(outputTopic));
    List<KeyValue<String, Long>> actualWordCounts = IntegrationTestUtils.readKeyValues(consumer);

    assertThat(actualWordCounts).containsExactlyElementsOf(expectedWordCounts);
  }

}
