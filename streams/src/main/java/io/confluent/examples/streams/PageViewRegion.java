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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
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

import java.util.Properties;

/**
 * Compute the number of pageviews by geo-region.
 */
public class PageViewRegion {

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

        KStream<String, GenericRecord> views = builder.stream(new ByteArrayDeserializer(), new KStreamAvroDeserializer(), "PageViews")
                // map user id as key
                .map((key, valueMap) -> new KeyValue((String) valueMap.get("user"), valueMap));

        KTable<String, String> users = builder.table(
                    keySerializer,
                    new KStreamAvroSerializer(),
                    keyDeserializer,
                    new KStreamAvroDeserializer(),
                    "UserProfile")
                .mapValues(record -> (String) record.get("region"));


        KStream<String, GenericRecord> stream2 = views.leftJoin(users, (view, region) -> new GenericRecord() {
            @Override
            public void put(String s, Object o) {

            }

            @Override
            public Object get(String s) {
                return null;
            }

            @Override
            public void put(int i, Object o) {

            }

            @Override
            public Object get(int i) {
                return null;
            }

            @Override
            public Schema getSchema() {
                return null;
            }
        });

        stream2.map((userId, geoViewRecord -> new KeyValue<PageId, Region>(view.pageId, geoViewRecord.region))
                .through("GeoPageViews") // repartition by page
                .countByKey(new Count(), HoppingWindows.of("GeoPageViewsWindow").with(7 * 24 * 60 * 60 * 1000))
                .toStream((pageId, count, window) -> new KeyValue<pageId, WeeklyCount>(pageId, new WeeklyCount(count, window.start()))
                                .to("PageAccessByRegion")


        topAriclesByIndustry.to("TopNewsPerIndustry");

        KafkaStreaming kstream = new KafkaStreaming(builder, config);
        kstream.start();
    }
}
