package io.confluent.examples.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.KeyValue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility functions to make integration testing more convenient.
 */
public class IntegrationTestUtils {

  private static final int UNLIMITED_MESSAGES = -1;

  /**
   * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from are
   * already configured in the consumer).
   * @param consumer
   * @param maxMessages Maximum number of messages to read via the consumer.
   * @return The values retrieved via the consumer.
   */
  public static <K, V> List<V> readValues(KafkaConsumer<K, V> consumer, int maxMessages) {
    List<KeyValue<K, V>> kvs = readKeyValues(consumer, maxMessages);
    return kvs.stream().map(kv -> kv.value).collect(Collectors.toList());
  }

  /**
   * Reading as many messages as possible via the provided consumer (the topic(s) to read from are
   * already configured in the consumer) until a (currently hardcoded) timeout is reached.
   * @param consumer
   * @return The KeyValue elements retrieved via the consumer.
   */
  public static <K, V> List<KeyValue<K, V>> readKeyValues(KafkaConsumer<K, V> consumer) {
    return readKeyValues(consumer, UNLIMITED_MESSAGES);
  }

  /**
   * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from are
   * already configured in the consumer).
   * @param consumer
   * @param maxMessages Maximum number of messages to read via the consumer.
   * @return The KeyValue elements retrieved via the consumer.
   */
  public static <K, V> List<KeyValue<K, V>> readKeyValues(KafkaConsumer<K, V> consumer, int maxMessages) {
    int pollIntervalMs = 100;
    int maxTotalPollTimeMs = 2000;
    int totalPollTimeMs = 0;
    List<KeyValue<K, V>> consumedValues = new ArrayList<>();
    while (totalPollTimeMs < maxTotalPollTimeMs && continueConsuming(consumedValues.size(), maxMessages)) {
      totalPollTimeMs += pollIntervalMs;
      ConsumerRecords<K, V> records = consumer.poll(pollIntervalMs);
      for (ConsumerRecord<K, V> record : records) {
        consumedValues.add(new KeyValue<>(record.key(), record.value()));
      }
    }
    return consumedValues;
  }

  private static boolean continueConsuming(int messagesConsumed, int maxMessages) {
    if (maxMessages <= 0) {
      return true;
    } else {
      return messagesConsumed < maxMessages;
    }
  }

}