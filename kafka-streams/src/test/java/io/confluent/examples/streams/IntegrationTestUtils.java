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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import kafka.utils.CoreUtils;

/**
 * Utility functions to make integration testing more convenient.
 */
public class IntegrationTestUtils {

  private static final int UNLIMITED_MESSAGES = -1;

  /**
   * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from are
   * already configured in the consumer).
   * @param consumer Kafka consumer instance
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
   * @param consumer Kafka consumer instance
   * @return The KeyValue elements retrieved via the consumer.
   */
  public static <K, V> List<KeyValue<K, V>> readKeyValues(KafkaConsumer<K, V> consumer) {
    return readKeyValues(consumer, UNLIMITED_MESSAGES);
  }

  /**
   * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from are
   * already configured in the consumer).
   * @param consumer Kafka consumer instance
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
    return maxMessages <= 0 || messagesConsumed < maxMessages;
  }

  /**
   * Removes local state stores.  Useful to reset state in-between integration test runs.
   * @param streamsConfiguration Streams configuration settings
   * @throws IOException
   */
  public static void purgeLocalStreamsState(Properties streamsConfiguration) throws IOException {
    String path = streamsConfiguration.getProperty(StreamsConfig.STATE_DIR_CONFIG);
    if (path != null) {
      File node = Paths.get(path).normalize().toFile();
      // Only purge state when it's under /tmp.  This is a safety net to prevent accidentally
      // deleting important local directory trees.
      if (node.getAbsolutePath().startsWith("/tmp")) {
        List<String> nodes = Collections.singletonList(node.getAbsolutePath());
        CoreUtils.delete(scala.collection.JavaConversions.asScalaBuffer(nodes).seq());
      }
    }
  }

}