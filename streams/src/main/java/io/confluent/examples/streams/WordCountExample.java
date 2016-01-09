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

import org.apache.avro.generic.GenericRecord;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WordCountExample {

    public static Map<String, Object> parse(GenericRecord record) {

        String title = (String) record.get("title");
        String flags = (String) record.get("flags");
        String diffUrl = (String) record.get("diffUrl");
        String user = (String) record.get("user");
        int byteDiff = Integer.parseInt((String) record.get("byteDiff"));
        String summary = (String) record.get("summary");

        Map<String, Boolean> flagMap = new HashMap<>();

        flagMap.put("is-minor", flags.contains("M"));
        flagMap.put("is-new", flags.contains("N"));
        flagMap.put("is-unpatrolled", flags.contains("!"));
        flagMap.put("is-bot-edit", flags.contains("B"));
        flagMap.put("is-special", title.startsWith("Special:"));
        flagMap.put("is-talk", title.startsWith("Talk:"));

        Map<String, Object> root = new HashMap<>();

        root.put("title", title);
        root.put("user", user);
        root.put("unparsed-flags", flags);
        root.put("diff-bytes", byteDiff);
        root.put("diff-url", diffUrl);
        root.put("summary", summary);
        root.put("flags", flagMap);

        return root;
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamingConfig.JOB_ID_CONFIG, "wordcount-example");
        props.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, KStreamAvroSerializer.class);
        props.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KStreamAvroDeserializer.class);
        props.put(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, SystemTimestampExtractor.class);

        StringSerializer keySerializer = new StringSerializer();
        StringDeserializer keyDeserializer = new StringDeserializer();

        StreamingConfig config = new StreamingConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        // read the source stream
        KStream<Object, GenericRecord> feeds = builder.stream("WikipediaFeed");

        // aggregate the new feed counts of by user
        KTable<Windowed<String>, Long> aggregated = feeds
                // parse the record
                .mapValues(value -> parse(value))
                // filter out old feeds
                .filter((key, valueMap) -> ((Map<String, Boolean>) valueMap.get("flags")).get("is-new"))
                // map the user as the key
                .map((key, valueMap) -> new KeyValue(valueMap.get("user"), valueMap))
                // sum by key
                .countByKey(HoppingWindows.of("window").with(60000L), keySerializer, keyDeserializer);


        // write to the result topic
        aggregated.to("WikipediaStats");

        KafkaStreaming kstream = new KafkaStreaming(builder, config);
        kstream.start();
    }
}