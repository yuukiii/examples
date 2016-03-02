/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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
import org.apache.kafka.streams.processor.internals.WallclockTimestampExtractor;

import java.util.Properties;

/**
 * Demonstrates how to perform simple, state-less transformations via map functions.
 *
 * Use cases include e.g. basic data sanitization, data anonymization by obfuscating sensitive data
 * fields (such as personally identifiable information aka PII).
 *
 * NOTE: this program works with Java 8 with lambda expression only.
 */
public class MapFunctionLambdaExample {

  public static void main(String[] args) throws Exception {
    KStreamBuilder builder = new KStreamBuilder();

    final Serializer<String> stringSerializer = new StringSerializer();
    final Deserializer<byte[]> byteArrayDeserializer = new ByteArrayDeserializer();
    final Deserializer<String> stringDeserializer = new StringDeserializer();

    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.JOB_ID_CONFIG, "streams-map-function-example");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    streamsConfiguration.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    streamsConfiguration.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    streamsConfiguration.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    streamsConfiguration.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    streamsConfiguration.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

    // Read the input Kafka topic into a KStream instance.
    KStream<byte[], String> textLines = builder.stream(
        byteArrayDeserializer,
        stringDeserializer,
        "TextLinesTopic");

    // Variant 1: using `mapValues`
    KStream<byte[], String> uppercasedWithMapValues = textLines.mapValues(String::toUpperCase);

    // Write (i.e. persist) the results to a new Kafka topic called "UppercasedTextLinesTopic".
    //
    // In this case we can rely on the default serializers for keys and values because their data
    // types did not change, i.e. we only need to provide the name of the output topic.
    uppercasedWithMapValues.to("UppercasedTextLinesTopic");

    // Variant 2: using `map`, modify value only (equivalent to variant 1)
    KStream<byte[], String> uppercasedWithMap = textLines.map((key, value) -> new KeyValue<>(key, value.toUpperCase()));

    // Variant 3: using `map`, modify both key and value
    //
    // Note: Whether, in general, you should follow this artificial example and store the original
    //       value in the key field is debatable and depends on your use case.  If in doubt, don't
    //       do it.
    KStream<String, String> originalAndUppercased = textLines.map((key, value) -> KeyValue.pair(value, value.toUpperCase()));

    // Write the results to a new Kafka topic "OriginalAndUppercased".
    //
    // In this case we must explicitly set the correct serializers because the default serializers
    // (cf. streaming configuration) do not match the type of this particular KStream instance.
    originalAndUppercased.to("OriginalAndUppercased", stringSerializer, stringSerializer);

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();
  }

}