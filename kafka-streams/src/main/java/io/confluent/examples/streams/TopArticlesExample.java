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

import io.confluent.examples.streams.utils.CollectionDeserializer;
import io.confluent.examples.streams.utils.CollectionSerializer;
import io.confluent.examples.streams.utils.GenericAvroDeserializer;
import io.confluent.examples.streams.utils.GenericAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;

/**
 * Create a feed of the top 100 news articles per industry ranked by
 * click-through-rate (assuming this is for the past week).
 */
public class TopArticlesExample {

    public static boolean isArticle(GenericRecord record) {
        String flags = (String) record.get("flags");

        return flags.contains("ART");
    }

    public static void main(String[] args) throws Exception {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.JOB_ID_CONFIG, "top-articles-example");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        streamsConfiguration.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        streamsConfiguration.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, GenericAvroSerializer.class);
        streamsConfiguration.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        streamsConfiguration.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class);

        final StringSerializer stringSerializer = new StringSerializer();
        final StringDeserializer stringDeserializer = new StringDeserializer();
        final Serializer<Long> longSerializer = new LongSerializer();
        final Deserializer<Long> longDeserializer = new LongDeserializer();
        final Serializer<GenericRecord> avroSerializer = new GenericAvroSerializer();
        final Deserializer<GenericRecord> avroDeserializer = new GenericAvroDeserializer();

        KStreamBuilder builder = new KStreamBuilder();

        KStream<byte[], GenericRecord> views = builder.stream("PageViews");

        KStream<GenericRecord, GenericRecord> articleViews = views
                // filter only article pages
                .filter((dummy, record) -> isArticle(record))
                // map <page id, industry> as key
                .map((dummy, article) -> new KeyValue<>(article, article));

        Schema schema = new Schema.Parser().parse(new File("pageviewstats.avsc"));

        KTable<Windowed<GenericRecord>, Long> viewCounts = articleViews
                // count the clicks within one week
                .countByKey(HoppingWindows.of("PageViewCountWindows").with(7 * 24 * 60 * 60 * 1000),
                    avroSerializer, longSerializer, avroDeserializer, longDeserializer);

        int topN = 100;
        KTable<Windowed<String>, Collection<GenericRecord>> topViewCounts = viewCounts
                .aggregate(
                     // initial value
                     Collections::<GenericRecord>emptySet,
                     // the "add" aggregator
                     new Aggregator<Windowed<String>, GenericRecord, Collection<GenericRecord>>() {

                         private final Map<Windowed<String>, PriorityQueue<GenericRecord>> sorted = new HashMap<>();

                         @Override
                         public Collection<GenericRecord> apply(Windowed<String> aggKey, GenericRecord value, Collection<GenericRecord> aggregate) {
                             PriorityQueue<GenericRecord> queue = sorted.get(aggKey);

                             if (queue == null) {
                                 queue = new PriorityQueue<>();
                                 sorted.put(aggKey, queue);
                             }
                             queue.add(value);

                             PriorityQueue<GenericRecord> copy = new PriorityQueue<>(queue);
                             Set<GenericRecord> ret = new HashSet<>();
                             for (int i = 1; i <= topN; i++) {
                                 ret.add(copy.poll());
                             }
                            return ret;
                         }
                     },
                     // the "remove" aggregator
                     new Aggregator<Windowed<String>, GenericRecord, Collection<GenericRecord>>() {

                       // FIXME: `sorted` must be shared between the `add` (above) and the `remove` (= this) aggregator
                       private final Map<Windowed<String>, PriorityQueue<GenericRecord>> sorted = new HashMap<>();

                       @Override
                       public Collection<GenericRecord> apply(Windowed<String> aggKey, GenericRecord value, Collection<GenericRecord> aggregate) {
                           PriorityQueue<GenericRecord> queue = sorted.get(aggKey);

                           if (queue == null) {
                              throw new IllegalStateException("This should not happen.");
                           }
                           queue.remove(value);

                           PriorityQueue<GenericRecord> copy = new PriorityQueue<>(queue);
                           Set<GenericRecord> ret = new HashSet<>();
                           for (int i = 1; i <= topN; i++) {
                              ret.add(copy.poll());
                           }
                           return ret;
                       }
                     },
                     // the selector
                     (windowedArticle, count) -> {
                         // project on the industry field for key
                         Windowed<String> windowedIndustry = new Windowed<>((String) windowedArticle.value().get("industry"), windowedArticle.window());
                         // add the page into the value
                         GenericRecord viewStats = new GenericData.Record(schema);
                         viewStats.put("page", "pageId");
                         viewStats.put("industry", "industryName");
                         viewStats.put("count", count);
                         return new KeyValue<>(windowedIndustry, viewStats);
                     },
                     new WindowedSerializer<>(stringSerializer),
                     null,
                     new CollectionSerializer<>(),
                     new WindowedDeserializer<>(stringDeserializer),
                     null,
                     new CollectionDeserializer<>(),
                     "Top100Articles");

        topViewCounts.to("TopNewsPerIndustry");

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
    }
}
