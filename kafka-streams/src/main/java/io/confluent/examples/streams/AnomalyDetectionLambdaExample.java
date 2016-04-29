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

import io.confluent.examples.streams.utils.GenericAvroSerde;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;

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
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "anomaly-detection-lambda-example");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // Where to find the Confluent schema registry instance(s)
        streamsConfiguration.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);

        KStreamBuilder builder = new KStreamBuilder();

        // read the source stream
        KStream<String, GenericRecord> views = builder.stream("PageViews");

        KStream<String, Long> anomalyUsers = views
                // map the user id as key
                .map((dummy, record) -> new KeyValue<>((String) record.get("user"), record))
                // count users, using one-minute tumbling windows
                .countByKey(TimeWindows.of("PageViewCountWindow", 60 * 1000L))
                // get users whose one-minute count is larger than 40
                .filter((windowedUserId, count) -> count > 40)
                // get rid of windows by transforming to a stream
                .toStream()
                .map((windowedUserId, count) -> new KeyValue<>(windowedUserId.key(), count));

        // write to the result topic
        anomalyUsers.to("AnomalyUsers");

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
    }

}