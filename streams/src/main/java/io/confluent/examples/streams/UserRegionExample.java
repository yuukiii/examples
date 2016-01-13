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
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreaming;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

/**
 * From the changelog stream of user profile data to compute the number of users
 * with complete profiles per region for regions having at least 10M users with
 * complete profiles. "Completeness" is defined as being at least 200 characters total.
 */
public class UserRegionExample {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamingConfig.JOB_ID_CONFIG, "regiongroup-example");
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

        // read the source stream
        KTable<String, GenericRecord> profile = builder.table("UserProfile");

        // aggregate the user counts of by region
        KTable<String, Long> regionCount = profile
                // filter out incomplete profiles with less than 200 characters
                .filter((userId, record) -> ((String) record.get("experience")).getBytes().length > 200)
                // count by region
                .count((userId, record) -> (String) record.get("region"), keySerializer, keyDeserializer, "CountsByRegion")
                // filter out regions with less than 10M users
                .filter((regionName, count) -> count > 10 * 1000 * 1000);

        // write to the result topic
        regionCount.to("LargeCountsByRegion");

        KafkaStreaming kstream = new KafkaStreaming(builder, config);
        kstream.start();
    }
}
