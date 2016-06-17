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

import io.confluent.examples.streams.utils.GenericAvroSerde;
import io.confluent.examples.streams.utils.WindowedSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;

/**
 * Demonstrates how to perform a join between a KStream and a KTable, i.e. an example of a stateful
 * computation, using the generic Avro binding for serdes in Kafka Streams.  Same as
 * PageViewRegionExample but uses lambda expressions and thus only works on Java 8+.
 *
 * In this example, we join a stream of page views (aka clickstreams) that reads from a topic named
 * "PageViews" with a user profile table that reads from a topic named "UserProfiles" to compute the
 * number of page views per user region.
 *
 * Note: Before running this example you must
 * 1) Start Zookeeper, Kafka and Schema Registry
 *    please refer to <a href='http://docs.confluent.io/3.0.0/quickstart.html#quickstart'>CP3.0.0
 *    QuickStart
 *    </a>
 * 2) create the topics e.g.
 * bin/kafka-topics --create --topic PageViews --zookeeper localhost:2181 --partitions 1
 *  --replication-factor 1
 * bin/kafka-topics --create --topic PageViewsByUser --zookeeper localhost:2181 --partitions 1
 * --replication-factor 1
 * bin/kafka-topics --create --topic UserProfile --zookeeper localhost:2181 --partitions 1
 * --replication-factor 1
 * bin/kafka-topics --create --topic PageViewsByRegion --zookeeper localhost:2181 --partitions 1
 * --replication-factor 1
 *
 *  (Note the above commands are for CP3.0.0 only. For Apache Kafka it should be `bin/kafka-topics
 *  .sh ...`)
 *
 * 3) start this example either in your IDE or on the command line. If via the command line please
 * refer to:
 * <a href='https://github.com/confluentinc/examples/tree/master/kafka-streams#packaging-and-running'>Packaging</a>
 * Once packaged you can then run:
 * java -cp -cp target/streams-examples-3.0.0-standalone.jar io.confluent.examples.streams.PageViewRegionLambdaExample
 *
 * 4) write some data to the source topics (e.g. via `kafka-avro-console-producer` or
 * {@link PageViewRegionExampleDriver}. Otherwise you won't see any data
 * arriving in the output topic.
 *
 *
 * Note: The generic Avro binding is used for serialization/deserialization.  This means the
 * appropriate Avro schema files must be provided for each of the "intermediate" Avro classes, i.e.
 * whenever new types of Avro objects (in the form of GenericRecord) are being passed between
 * processing steps.
 */
public class PageViewRegionLambdaExample {

    public static void main(String[] args) throws Exception {
        Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "pageview-region-lambda-example");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // Where to find the Confluent schema registry instance(s)
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();
        final Serde<Windowed<String>> windowedStringSerde = new WindowedSerde<>(stringSerde);

        KStreamBuilder builder = new KStreamBuilder();

        // Create a stream of page view events from the PageViews topic, where the key of
        // a record is assumed to be the user id (String) and the value an Avro GenericRecord
        // that represents the full details of the page view event.  See `pageview.avsc` under
        // `src/main/avro/` for the corresponding Avro schema.
        KStream<String, GenericRecord> views = builder.stream("PageViews");

        KStream<String, GenericRecord> viewsByUser = views
            .map((dummy, record) ->
                     new KeyValue<>(record.get("user").toString(), record))
            .through("PageViewsByUser");

        // Create a changelog stream for user profiles from the UserProfiles topic,
        // where the key of a record is assumed to be the user id (String) and its value
        // an Avro GenericRecord.  See `userprofile.avsc` under `src/main/avro/` for the
        // corresponding Avro schema.
        KTable<String, GenericRecord> users = builder.table("UserProfile");

        KTable<String, String> userRegions = users.mapValues(record ->
                                                                 record.get("region").toString());

        // We must specify the Avro schemas for all intermediate (Avro) classes, if any.
        // In this example, we want to create an intermediate GenericRecord to hold the view region.
        // See `pageviewregion.avsc` under `src/main/avro/`.
        final InputStream
            pageViewRegionSchema =
            PageViewRegionLambdaExample.class.getClassLoader()
                .getResourceAsStream("avro/io/confluent/examples/streams/pageviewregion.avsc");
        Schema schema = new Schema.Parser().parse(pageViewRegionSchema);

        KTable<Windowed<String>, Long> regionCount = viewsByUser
                .leftJoin(userRegions, (view, region) -> {
                    GenericRecord viewRegion = new GenericData.Record(schema);
                    viewRegion.put("user", view.get("user"));
                    viewRegion.put("page", view.get("page"));
                    viewRegion.put("region", region);
                    return viewRegion;
                })
                .map((user, viewRegion) -> new KeyValue<>(viewRegion.get("region").toString(), viewRegion))
                // count views by user, using hopping windows of size 5 minutes that advance every 1 minute
                .countByKey(TimeWindows.of("GeoPageViewsWindow", 5 * 60 * 1000L).advanceBy(60 * 1000L));

        regionCount.to(windowedStringSerde, longSerde, "PageViewsByRegion");

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
    }

}
