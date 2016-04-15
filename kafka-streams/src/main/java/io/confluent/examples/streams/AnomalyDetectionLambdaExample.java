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

import io.confluent.examples.streams.utils.GenericAvroDeserializer;
import io.confluent.examples.streams.utils.GenericAvroSerializer;
import org.apache.avro.generic.GenericRecord;
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
import org.apache.kafka.streams.kstream.TumblingWindows;

import java.util.Properties;

/**
 * Detect users doing more than 40 page views per minute from the pageview stream
 * and output a stream of blocked users (assuming a sliding window of 1 minute).
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class AnomalyDetectionLambdaExample {

    public static void main(String[] args) throws Exception {
        Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.JOB_ID_CONFIG, "anomaly-detection-lambda-example");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // Where to find the Confluent schema registry instance(s)
        streamsConfiguration.put("schema.registry.url", "http://localhost:8081");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        streamsConfiguration.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, GenericAvroSerializer.class);
        streamsConfiguration.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        streamsConfiguration.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class);

        final Serializer<Long> longSerializer = new LongSerializer();
        final Deserializer<Long> longDeserializer = new LongDeserializer();

        KStreamBuilder builder = new KStreamBuilder();

        // read the source stream
        KStream<String, GenericRecord> views = builder.stream("PageViews");

        KStream<String, Long> anomalyUsers = views
                // map the user id as key
                .map((dummy, record) -> new KeyValue<>((String) record.get("user"), record))
                // count users on the one-minute tumbling window
                .countByKey(TumblingWindows.of("PageViewCountWindow").with(60 * 1000L),
                    null, longSerializer, null, longDeserializer)
                // get users whose one-minute count is larger than 40
                .filter((windowedUserId, count) -> count > 40)
                // get rid of windows by transforming to a stream
                .toStream()
                .map((windowedUserId, count) -> new KeyValue<>(windowedUserId.value(), count));

        // write to the result topic
        anomalyUsers.to("AnomalyUsers");

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
    }

}