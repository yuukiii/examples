package io.confluent.examples.streams;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
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
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.UnlimitedWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import kafka.utils.CoreUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class WordCountIntegrationTest {

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
  public void shouldCountWords() throws Exception {
    List<String> inputValues = Arrays.asList("hello", "world", "world", "hello world");
    List<KeyValue<String, Long>> expectedValues = Arrays.asList(
        new KeyValue<>("hello", 1L),
        new KeyValue<>("world", 1L),
        new KeyValue<>("world", 2L),
        new KeyValue<>("hello", 2L),
        new KeyValue<>("world", 3L)
    );

    //
    // Step 1: Configure and start the Streams job.
    //
    final Serializer<String> stringSerializer = new StringSerializer();
    final Deserializer<String> stringDeserializer = new StringDeserializer();
    final Serializer<Long> longSerializer = new LongSerializer();
    final Deserializer<Long> longDeserializer = new LongDeserializer();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.JOB_ID_CONFIG, "wordcount-integration-test");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
    streamsConfiguration.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    streamsConfiguration.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    streamsConfiguration.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    streamsConfiguration.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

    // Remove any state from previous test runs
    purgeLocalStreamsState(streamsConfiguration);

    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, String> source = builder.stream(inputTopic);

    KStream<String, Long> counts = source
        .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
        .map((key, value) -> new KeyValue<>(value, value))
        .countByKey(UnlimitedWindows.of("Counts").startOn(0L),
            stringSerializer, longSerializer, stringDeserializer, longDeserializer)
        .toStream()
        .map((windowedKey, count) -> new KeyValue<>(windowedKey.value(), count));

    counts.to(outputTopic, stringSerializer, longSerializer);

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
    consumerConfig.put("group.id", "wordcount-integration-test-standard-consumer");
    consumerConfig.put("auto.offset.reset", "earliest");
    consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");

    KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerConfig);
    consumer.subscribe(Collections.singletonList(outputTopic));
    List<KeyValue<String, Long>> actualValues = IntegrationTestUtils.readKeyValues(consumer);

    // Note: This assert statement will work once we have merged the PR for equality semantics of
    //       `KeyValue`.  See https://github.com/apache/kafka/pull/872.
    assertThat(actualValues).containsExactlyElementsOf(expectedValues);
  }

  private static void purgeLocalStreamsState(Properties streamingConfiguration) throws IOException {
    String path = streamingConfiguration.getProperty(StreamsConfig.STATE_DIR_CONFIG);
    if (path != null) {
      File node = Paths.get(path).normalize().toFile();
      // Only purge state when it's under /tmp.  This is a safety net to prevent accidentally
      // deleting important local directory trees.
      if (node.getAbsolutePath().startsWith("/tmp")) {
        CoreUtils.rm(node);
      }
    }
  }

}