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

import io.confluent.examples.streams.utils.GenericAvroDeserializer;
import io.confluent.examples.streams.utils.GenericAvroSerializer;
import io.confluent.examples.streams.utils.SystemTimestampExtractor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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

import java.io.File;
import java.util.Properties;

/**
 * Compute the number of pageviews by geo-region.
 */
public class PageViewRegion {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamingConfig.JOB_ID_CONFIG, "anomalydetection-example");
        props.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, GenericAvroSerializer.class);
        props.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class);
        props.put(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, SystemTimestampExtractor.class);

        StringSerializer keySerializer = new StringSerializer();
        StringDeserializer keyDeserializer = new StringDeserializer();

        StreamingConfig config = new StreamingConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, GenericRecord> views = builder.stream("PageViews");

        KStream<String, GenericRecord> viewsByUser = views.map((dummy, record) -> new KeyValue<>((String) record.get("user"), record));

        KTable<String, GenericRecord> users = builder.table("UserProfile");

        KTable<String, String> userRegions = users.mapValues(record -> (String) record.get("region"));

        Schema schema = new Schema.Parser().parse(new File("pageviewregion.avsc"));

        KTable<Windowed<String>, Long> regionCount = viewsByUser
                .leftJoin(userRegions, (view, region) -> {
                    GenericRecord viewRegion = new GenericData.Record(schema);
                    viewRegion.put("user", view.get("user"));
                    viewRegion.put("page", view.get("page"));
                    viewRegion.put("region", region);

                    return viewRegion;
                })
                .map((user, viewRegion) -> new KeyValue<>((String) viewRegion.get("region"), viewRegion))
                .countByKey(HoppingWindows.of("GeoPageViewsWindow").with(7 * 24 * 60 * 60 * 1000), keySerializer, keyDeserializer);

        // write to the result topic
        regionCount.to("PageViewsByRegion");

        KafkaStreaming kstream = new KafkaStreaming(builder, config);
        kstream.start();
    }
}
