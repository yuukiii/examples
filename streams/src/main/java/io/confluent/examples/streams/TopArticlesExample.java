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

import io.confluent.examples.streams.utils.KStreamAvroDeserializer;
import io.confluent.examples.streams.utils.KStreamAvroSerializer;
import io.confluent.examples.streams.utils.SystemTimestampExtractor;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
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
import org.apache.kafka.streams.kstream.internals.DefaultWindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.DefaultWindowedSerializer;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 * Create a feed of the top 100 news articles per industry ranked by
 * click-through-rate (assuming this is for the past week).
 */
public class TopArticlesExample {

    public static String extractIndustry(GenericRecord record) {
        return (String) record.get("industry");
    }

    public static String extractPageId(GenericRecord record) {
        return (String) record.get("pageId");
    }

    public static boolean isArticle(GenericRecord record) {
        String flags = (String) record.get("flags");

        return flags.contains("ART");
    }

    private static class ArticleStats implements Comparable<ArticleStats> {

        public String pageId;
        public long counts;

        public ArticleStats(String pageId, long counts) {
            this.pageId = pageId;
            this.counts = counts;
        }

        @Override
        public int compareTo(ArticleStats o) {
            return (int) (this.counts - o.counts);
        }
    }

    private static class ArticleStatsSerializer implements Serializer<ArticleStats> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // do nothing
        }

        public byte[] serialize(String topic, ArticleStats data) {

            return data.pageId.getBytes(); // TODO: this is just a placeholder that will not work
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    private static class ArticleStatsDeserializer implements Deserializer<ArticleStats> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // do nothing
        }

        public ArticleStats deserialize(String topic, byte[] data) {
            return new ArticleStats("page-id", 0L);  // TODO: this is just a placeholder that will not work
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamingConfig.JOB_ID_CONFIG, "anomalydetection-example");
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

        KStream<String, GenericRecord> articles = builder.stream(new ByteArrayDeserializer(), new KStreamAvroDeserializer(), "PageViews")
                //  filter only article pages
                .filter((dummy, record) -> isArticle(record))
                // map page id as key
                .map((dummy, article) -> new KeyValue<>(extractPageId(article), article));


        KTable<Windowed<String>, Collection<ArticleStats>> topAriclesByIndustry = articles
                // count the clicks within one week
                .countByKey(HoppingWindows.of("PageViewCountWindows").with(7 * 24 * 60 * 60 * 1000), keySerializer, keyDeserializer)
                // add the article id as part of the value as well
                .mapValues(count -> new ArticleStats("article", count))    // TODO: add key to value
                // get the top-100 articles by industry (assuming it is the first substring of page id)
                .topK(100, (windowedPageId, stats) -> new Windowed<>(windowedPageId.value().substring(0, 10), windowedPageId.window()),
                        new DefaultWindowedSerializer<>(keySerializer),
                        new ArticleStatsSerializer(),
                        new DefaultWindowedDeserializer<>(keyDeserializer),
                        new ArticleStatsDeserializer(), "Top100Articles");

        topAriclesByIndustry.to("TopNewsPerIndustry");

        KafkaStreaming kstream = new KafkaStreaming(builder, config);
        kstream.start();
    }
}
