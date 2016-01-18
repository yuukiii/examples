package io.confluent.examples.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreaming;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.examples.streams.utils.SystemTimestampExtractor;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests whether Streams is able to read data from an input topic and write the same data (as-is) to
 * a new output topic.
 *
 * THE CODE IS A WORK IN PROGRESS AND NOT WORKING CORRECTLY YET.
 *
 * The logging output of the test includes the location of Kafka's log.dirs, which you can use to
 * quickly inspect the contents of the related topics/partitions (see `testInputTopic` and
 * `testOutputTopic`).
 *
 * Logging output example:
 *
 *    DEBUG [main] logs.dir at /var/folders/j3/1y7_mtp51fn8xh1h9ff3kt3m0000gn/T/kafka-embedded-logs-dir-1511050635 was not removed
 *
 */
public class NoOpStreamsIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(NoOpStreamsIntegrationTest.class);

  private static EmbeddedSingleNodeKafkaCluster cluster = null;
  private static String testInputTopic = "inputTopic";
  private static String testOutputTopic = "outputTopic";

  @BeforeClass
  public static void startKafkaCluster() throws Exception {
    cluster = new EmbeddedSingleNodeKafkaCluster();
    cluster.createTopic(testInputTopic);
    cluster.createTopic(testOutputTopic);
  }

  @AfterClass
  public static void stopKafkaCluster() throws IOException {
    if (cluster != null) {
      cluster.stop();
    }
  }

  @Test
  public void shouldWriteTheInputDataAsIsToTheOutputTopic() throws Exception {
    //
    // The test input data.
    //
    List<String> lines = Arrays.asList(
        "hello world",
        "the world is not enough",
        "the world of the stock market is coming to an end"
    );

    //
    // Configure and start the Streams job.
    //
    KStreamBuilder builder = new KStreamBuilder();
    KStream<String, String> textLines = builder.stream(testInputTopic);

    // write the input data as-is to the output topic
    textLines.to(testOutputTopic);
    // Alternatively, modify the input data with a trivial transformation
    //textLines.mapValues(String::toUpperCase).to(testOutputTopic);

    Properties streamingConfiguration = new Properties();
    streamingConfiguration.put(StreamingConfig.JOB_ID_CONFIG, "noop-test-streams");
    streamingConfiguration.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
    streamingConfiguration.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    streamingConfiguration.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    streamingConfiguration.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    streamingConfiguration.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    streamingConfiguration.put(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, SystemTimestampExtractor.class);
    //streamingConfiguration.put(StreamingConfig.TOTAL_RECORDS_TO_PROCESS, lines.size());
    streamingConfiguration.put(StreamingConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
    streamingConfiguration.put(StreamingConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "1");
    streamingConfiguration.put(StreamingConfig.WINDOW_TIME_MS_CONFIG, "100");

    KafkaStreaming kafkaStreaming = new KafkaStreaming(builder, new StreamingConfig(streamingConfiguration));
    kafkaStreaming.start();

    //
    // Produce some input data to the input topic
    //
    Properties producerConfig = new Properties();
    producerConfig.put("bootstrap.servers", cluster.bootstrapServers());
    producerConfig.put("acks", "all");
    producerConfig.put("retries", 0);
    producerConfig.put("batch.size", 1);
    producerConfig.put("linger.ms", 0);
    producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(producerConfig);
    for (String line : lines) {
      log.debug("Producing message with value '{}' to topic {}", line, testInputTopic);
      Future<RecordMetadata> f = producer.send(new ProducerRecord<>(testInputTopic, line));
      f.get();
    }
    producer.flush();
    producer.close();

    //
    // For comparison with Streams functionality, read the input topic also with the standard Kafka
    // consumer API.
    //
    Properties consumerConfig = new Properties();
    consumerConfig.put("bootstrap.servers", cluster.bootstrapServers());
    consumerConfig.put("group.id", "noop-test-standard-consumer");
    consumerConfig.put("enable.auto.commit", "true");
    consumerConfig.put("auto.offset.reset", "earliest"); // important!
    consumerConfig.put("auto.commit.interval.ms", "50");
    consumerConfig.put("session.timeout.ms", "30000");
    consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    log.debug("Consumer configuration: {}", consumerConfig);

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
    consumer.subscribe(Collections.singletonList(testInputTopic));
    int remainingMessages = lines.size();
    while (remainingMessages > 0) {
      ConsumerRecords<String, String> records = consumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        log.debug("Received message with offset = {}, key = {}, value = {}",
            record.offset(), record.key(), record.value());
        remainingMessages -= 1;
      }
    }

    Thread.sleep(5 * 1000);
    kafkaStreaming.close();

    // TODO: Actually implement some asserts.
    assertThat("foo").isEqualTo("foo");
  }

}