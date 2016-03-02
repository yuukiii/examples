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

import io.confluent.examples.streams.avro.WikiFeed;
import io.confluent.examples.streams.utils.SpecificAvroDeserializer;
import io.confluent.examples.streams.utils.SpecificAvroSerializer;

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
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.Properties;


/**
 * From the wiki feed irc stream compute the number of new user feeds for every minute.
 *
 * Note: The specific Avro binding is used for serialization/deserialization, where the `WikiFeed`
 * class is auto-generated from its Avro schema by the maven avro plugin.
 */
public class WordCountExample {

    public static void main(String[] args) throws Exception {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.JOB_ID_CONFIG, "wordcount-example");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        streamsConfiguration.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        streamsConfiguration.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
        streamsConfiguration.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        streamsConfiguration.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class);

        final Serializer<Long> longSerializer = new LongSerializer();
        final Deserializer<Long> longDeserializer = new LongDeserializer();
        final Serializer<String> stringSerializer = new StringSerializer();
        final Deserializer<String> stringDeserializer = new StringDeserializer();

        KStreamBuilder builder = new KStreamBuilder();

        // read the source stream
        KStream<String, WikiFeed> feeds = builder.stream("WikipediaFeed");

        // aggregate the new feed counts of by user
        KTable<String, Long> aggregated = feeds
                // filter out old feeds
                .filter(new Predicate<String, WikiFeed> () {
                    @Override
                    public boolean test(String dummy, WikiFeed value) {
                        return value.getIsNew();
                    }
                })
                // map the user id as key
                .map(new KeyValueMapper<String, WikiFeed, KeyValue<String, WikiFeed>>() {
                    @Override
                    public KeyValue<String, WikiFeed> apply(String key, WikiFeed value) {
                        return new KeyValue<>(value.getUser(), value);
                    }
                })
                // sum by key, need to override the serdes for String typed key
                .countByKey(stringSerializer, longSerializer, stringDeserializer, longDeserializer, "Counts");

        // write to the result topic, need to override serdes
        aggregated.to("WikipediaStats", stringSerializer, longSerializer);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
    }
}