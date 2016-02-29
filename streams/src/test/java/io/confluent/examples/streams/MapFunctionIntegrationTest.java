package io.confluent.examples.streams;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
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
import java.util.stream.Collectors;

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;

public class MapFunctionIntegrationTest {

  private static EmbeddedSingleNodeKafkaCluster cluster = null;
  private static String inputTopic = "inputTopic";
  private static String outputTopic = "outputTopic";

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
  public void shouldUppercaseTheInput() throws Exception {
    List<String> inputValues = Arrays.asList("hello", "world");
    List<String> expectedValues = inputValues.stream().map(String::toUpperCase).collect(Collectors.toList());

    //
    // Step 1: Configure and start the Streams job.
    //
    KStreamBuilder builder = new KStreamBuilder();

    // Write the input data as-is to the output topic.
    KStream<byte[], String> input = builder.stream(inputTopic);

    KStream<byte[], String> uppercased = input.mapValues(String::toUpperCase);
    uppercased.to(outputTopic);

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.JOB_ID_CONFIG, "map-function-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    streamsConfiguration.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    streamsConfiguration.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    streamsConfiguration.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();

    // Wait briefly for the streaming job to be fully up and running (otherwise it might miss
    // some or all of the input data we produce below).
    Thread.sleep(1000);

    //
    // Step 2: Produce some input data to the input topic.
    //
    Properties producerConfig = new Properties();
    producerConfig.put("bootstrap.servers", cluster.bootstrapServers());
    producerConfig.put("acks", "all");
    producerConfig.put("retries", 0);
    producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(producerConfig);
    for (String value : inputValues) {
      Future<RecordMetadata> f = producer.send(new ProducerRecord<>(inputTopic, value));
      f.get();
    }
    producer.flush();
    producer.close();

    // Give the streaming job some time to do its work.
    Thread.sleep(1000);
    streams.close();

    //
    // Step 3: Verify the job's output data.
    //
    Properties consumerConfig = new Properties();
    consumerConfig.put("bootstrap.servers", cluster.bootstrapServers());
    consumerConfig.put("group.id", "map-function-integration-test-standard-consumer");
    consumerConfig.put("auto.offset.reset", "earliest");
    consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
    consumer.subscribe(Collections.singletonList(outputTopic));
    List<String> actualValues = IntegrationTestUtils.readValues(consumer, inputValues.size());
    assertThat(actualValues).isEqualTo(expectedValues);
  }

}