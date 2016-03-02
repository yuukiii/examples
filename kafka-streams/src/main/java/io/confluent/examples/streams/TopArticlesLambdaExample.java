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

import io.confluent.examples.streams.utils.PriorityQueueDeserializer;
import io.confluent.examples.streams.utils.PriorityQueueSerializer;
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
import org.apache.kafka.streams.kstream.HoppingWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;

import java.io.File;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Properties;

/**
 * Create a feed of the top 100 news articles per industry ranked by
 * click-through-rate (assuming this is for the past week).
 *
 * NOTE: this program works with Java 8 with lambda expression only.
 */
public class TopArticlesLambdaExample {

    public static boolean isArticle(GenericRecord record) {
        String flags = (String) record.get("flags");

        return flags.contains("ART");
    }

    public static void main(String[] args) throws Exception {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.JOB_ID_CONFIG, "top-articles-lambda-example");
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
        final Serializer<Windowed<String>> windowedStringSerializer = new WindowedSerializer<>(stringSerializer);
        final Deserializer<Windowed<String>> windowedDeserializer = new WindowedDeserializer<>(stringDeserializer);

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

        KTable<Windowed<String>, PriorityQueue<GenericRecord>> allViewCounts = viewCounts
                .aggregate(
                        // the initializer
                        () -> {
                            Comparator<GenericRecord> comparator = new Comparator<GenericRecord>() {
                                @Override
                                public int compare(GenericRecord o1, GenericRecord o2) {
                                    return (int) ((Long) o1.get("count") - (Long) o2.get("count"));
                                }
                            };

                            return new PriorityQueue<>(comparator);
                        },

                        // the "add" aggregator
                        (windowedIndustry, record, queue) -> {
                            queue.add(record);
                            return queue;
                        },

                        // the "remove" aggregator
                        (windowedIndustry, record, queue) -> {
                            queue.remove(record);
                            return queue;
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

                        windowedStringSerializer,
                        avroSerializer,
                        new PriorityQueueSerializer<>(),
                        windowedDeserializer,
                        avroDeserializer,
                        new PriorityQueueDeserializer<>(),

                        "AllArticles"
                );

        int topN = 100;
        KTable<Windowed<String>, String> topViewCounts = allViewCounts
                .mapValues(queue -> {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < topN; i++) {
                        GenericRecord record = queue.poll();

                        if (record == null)
                            break;

                        sb.append((String) record.get("page"));
                        sb.append("\n");
                    }

                    return sb.toString();
                });

        topViewCounts.to("TopNewsPerIndustry", windowedStringSerializer, stringSerializer);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
    }
}
