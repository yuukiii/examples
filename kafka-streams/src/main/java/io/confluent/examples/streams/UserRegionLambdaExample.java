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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

/**
 * Computes, per region, the number of users with "complete" user profiles for such regions that
 * have at least 10 million users with complete profiles.  A user profile is naively considered
 * "complete" whenever it has a total of at least 200 characters.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class UserRegionLambdaExample {

    public static void main(String[] args) throws Exception {
        Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-region-lambda-example");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // Where to find the Confluent schema registry instance(s)
        streamsConfiguration.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStreamBuilder builder = new KStreamBuilder();

        // Read the source stream
        KTable<String, GenericRecord> profile = builder.table("UserProfile");

        // Aggregate the user counts of by region
        KTable<String, Long> regionCount = profile
            // Filter out incomplete profiles with less than 200 characters
            .filter((userId, record) -> ((String) record.get("experience")).getBytes().length > 200)
            // Count by region
            // We do not need to specify any explict serdes because the key and value types do not change
            .groupBy((userId, record) -> KeyValue.pair((String) record.get("region"), record))
            .count("CountsByRegion)")
            // filter out regions with less than 10M users
            .filter((regionName, count) -> count > 10 * 1000 * 1000);

        // write to the result topic, we need to override the value serializer to for type long
        regionCount.to(stringSerde, longSerde, "LargeCountsByRegion");

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
    }

}