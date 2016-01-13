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

import io.confluent.examples.streams.classes.WikiFeed;
import io.confluent.examples.streams.classes.WikiFeedAvroDeserializer;
import io.confluent.examples.streams.utils.GenericAvroDeserializer;
import io.confluent.examples.streams.utils.GenericAvroSerializer;
import io.confluent.examples.streams.utils.SystemTimestampExtractor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreaming;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Properties;


/**
 * From the wiki feed irc stream compute the number of new user feeds for every minute.
 */
public class WordCountExample {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamingConfig.JOB_ID_CONFIG, "wordcount-example");
        props.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, GenericAvroSerializer.class);
        props.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class);
        props.put(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, SystemTimestampExtractor.class);

        StreamingConfig config = new StreamingConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        // read the source stream
        KStream<byte[], WikiFeed> feeds = builder.stream(
                new ByteArrayDeserializer(),
                new WikiFeedAvroDeserializer(),
                "WikipediaFeed");

        // aggregate the new feed counts of by user
        StringSerializer keySerializer = new StringSerializer();
        StringDeserializer keyDeserializer = new StringDeserializer();
        KTable<Windowed<String>, Long> aggregated = feeds
                // filter out old feeds
                .filter((dummy, value) -> value.getIsNew())
                // map the user id as key
                .map((key, value) -> new KeyValue<>(value.getUser(), value))
                // sum by key
                .countByKey(HoppingWindows.of("window").with(60000L), keySerializer, keyDeserializer);

        // write to the result topic
        aggregated.to("WikipediaStats");

        KafkaStreaming kstream = new KafkaStreaming(builder, config);
        kstream.start();
    }
}