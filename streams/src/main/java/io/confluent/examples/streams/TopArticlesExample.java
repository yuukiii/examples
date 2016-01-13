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

import io.confluent.examples.streams.classes.CollectionDeserializer;
import io.confluent.examples.streams.classes.CollectionSerializer;
import io.confluent.examples.streams.utils.GenericAvroDeserializer;
import io.confluent.examples.streams.utils.GenericAvroSerializer;
import io.confluent.examples.streams.utils.SystemTimestampExtractor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaStreaming;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.DefaultWindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.DefaultWindowedSerializer;

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
        Properties props = new Properties();
        props.put(StreamingConfig.JOB_ID_CONFIG, "anomalydetection-example");
        props.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, GenericAvroSerializer.class);
        props.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class);
        props.put(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, SystemTimestampExtractor.class);

        GenericAvroSerializer genericAvroSerializer = new GenericAvroSerializer();
        GenericAvroDeserializer genericAvroDeserializer = new GenericAvroDeserializer();

        StreamingConfig config = new StreamingConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<byte[], GenericRecord> views = builder.stream("PageViews");

        KStream<GenericRecord, GenericRecord> articleViews = views
                // filter only article pages
                .filter((dummy, record) -> isArticle(record))
                // map <page id, industry> as key
                .map((dummy, article) -> new KeyValue<>(article, article));

        Schema schema = new Schema.Parser().parse(new File("pageviewstats.avsc"));

        KTable<Windowed<GenericRecord>, GenericRecord> viewCounts = articleViews
                // count the clicks within one week
                .countByKey(HoppingWindows.of("PageViewCountWindows").with(7 * 24 * 60 * 60 * 1000), genericAvroSerializer, genericAvroDeserializer)
                // add the article id as part of the value as well
                .mapValues(count -> {
                    GenericRecord viewStats = new GenericData.Record(schema);
                    viewStats.put("page", "pageId");        // TODO: add key to value
                    viewStats.put("industry", "industryName");        // TODO: add key to value
                    viewStats.put("count", count);

                    return viewStats;
                });

         KTable<Windowed<String>, Collection<GenericRecord>> topViewCounts = viewCounts
                 .aggregate(() -> new Aggregator<Windowed<String>, GenericRecord, Collection<GenericRecord>>() {
                            private final int k = 100;

                            private final Map<Windowed<String>, PriorityQueue<GenericRecord>> sorted = new HashMap<>();

                            @Override
                            public Collection<GenericRecord> initialValue() {
                                return Collections.<GenericRecord>emptySet();
                            }

                            @Override
                            public Collection<GenericRecord> add(Windowed<String> aggKey, GenericRecord value, Collection<GenericRecord> aggregate) {
                                PriorityQueue<GenericRecord> queue = sorted.get(aggKey);
                                if (queue == null) {
                                    queue = new PriorityQueue<>();
                                    sorted.put(aggKey, queue);
                                }

                                queue.add(value);

                                PriorityQueue<GenericRecord> copy = new PriorityQueue<>(queue);

                                Set<GenericRecord> ret = new HashSet<>();
                                for (int i = 1; i <= k; i++)
                                    ret.add(copy.poll());

                                return ret;
                            }

                            @Override
                            public Collection<GenericRecord> remove(Windowed<String> aggKey, GenericRecord value, Collection<GenericRecord> aggregate) {
                                PriorityQueue<GenericRecord> queue = sorted.get(aggKey);

                                if (queue == null)
                                    throw new IllegalStateException("This should not happen.");

                                queue.remove(value);

                                PriorityQueue<GenericRecord> copy = new PriorityQueue<>(queue);

                                Set<GenericRecord> ret = new HashSet<>();
                                for (int i = 1; i <= k; i++)
                                    ret.add(copy.poll());

                                return ret;
                            }

                            @Override
                            public Collection<GenericRecord> merge(Collection<GenericRecord> aggr1, Collection<GenericRecord> aggr2) {
                                PriorityQueue<GenericRecord> copy = new PriorityQueue<>(aggr1);
                                copy.addAll(aggr2);

                                Set<GenericRecord> ret = new HashSet<>();
                                for (int i = 1; i <= k; i++)
                                    ret.add(copy.poll());

                                return ret;
                            }
                        },
                        (windowedArticle, stats) -> {
                            Windowed<String> windowedIndustry = new Windowed<>((String) windowedArticle.value().get("industry"), windowedArticle.window());

                            return new KeyValue<>(windowedIndustry, stats);
                        },
                        new DefaultWindowedSerializer<>(genericAvroSerializer),
                        new CollectionSerializer<>(),
                        new DefaultWindowedDeserializer<>(genericAvroDeserializer),
                        new CollectionDeserializer<>(), "Top100Articles");

        topViewCounts.to("TopNewsPerIndustry");

        KafkaStreaming kstream = new KafkaStreaming(builder, config);
        kstream.start();
    }
}
