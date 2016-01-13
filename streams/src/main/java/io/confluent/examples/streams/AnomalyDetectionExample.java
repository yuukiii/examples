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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreaming;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Properties;

/**
 * Detect users doing more than 40 page views per minute from the pageview stream
 * and output a stream of blocked users (assuming a sliding window of 1 minute).
 */
public class AnomalyDetectionExample {

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

        // read the source stream
        KStream<byte[], GenericRecord> views = builder.stream("PageViews");

        KStream<String, Long> anomalyUsers = views
                // map the user id as key
                .map((dummy, record) -> new KeyValue<>((String) record.get("user"), record))
                // count users on the one-minute sliding window
                .countByKey(SlidingWindows.of("PageViewCountWindow").with(60 * 1000L), keySerializer, keyDeserializer)
                // get users whose one-minute count is larger than 40
                .filter((windowedUserId, count) -> (Long) count > 40)   // TODO: avoid casting
                // transform to streams and get rid of windows
                .toStream()
                .map((windowedUserId, count) -> new KeyValue<>(windowedUserId.value(), count));

        // write to the result topic
        anomalyUsers.to("AnomalyUsers");

        KafkaStreaming kstream = new KafkaStreaming(builder, config);
        kstream.start();
    }
}
