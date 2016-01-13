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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreaming;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValue;

import java.util.Properties;

import io.confluent.examples.streams.utils.SystemTimestampExtractor;

/**
 * Demonstrates how to perform simple, state-less transformations via map functions.
 *
 * Use cases include e.g. basic data sanitization, data anonymization by obfuscating sensitive data
 * fields (such as personally identifiable information aka PII).
 */
public class MapFunctionExample {

  public static void main(String[] args) throws Exception {
    KStreamBuilder builder = new KStreamBuilder();

    // Read the input Kafka topic into a KStream instance.
    KStream<byte[], String> textLines = builder.stream(
        new ByteArrayDeserializer(),
        new StringDeserializer(),
        "TextLinesTopic");

    // Variant 1: using `mapValues`
    KStream<byte[], String> uppercasedWithMapValues = textLines.mapValues(String::toUpperCase);

    // Write (i.e. persist) the results to a new Kafka topic called "UppercasedTextLinesTopic".
    //
    // Alternatively you could also explicitly set the serializers instead of relying on the default
    // serializers specified in the streaming configuration (see further down below):
    //
    //     uppercasedWithMapValues.to("UppercasedTextLinesTopic", new ByteArraySerializer(), new StringSerializer());
    //
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
    //
    // Note: Normally you would use a single instance of StringSerializer and pass it around.
    //       We are creating new instances purely for didactic reasons so that you do not need to
    //       jump around in the code too much in these examples.
    originalAndUppercased.to("OriginalAndUppercased", new StringSerializer(), new StringSerializer());

    Properties streamingConfiguration = new Properties();
    streamingConfiguration.put(StreamingConfig.JOB_ID_CONFIG, "map-function-example");
    streamingConfiguration.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    streamingConfiguration.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    streamingConfiguration.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    streamingConfiguration.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    streamingConfiguration.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    streamingConfiguration.put(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, SystemTimestampExtractor.class);

    KafkaStreaming stream = new KafkaStreaming(builder, new StreamingConfig(streamingConfiguration));
    stream.start();
  }

}