package io.confluent.examples.streams;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.Properties;

/**
 * Demonstrates how to reset a Kafka Streams application to re-process its input data from scratch.
 * <p>
 * The main purpose of the example is to explain the usage of the "Application Reset Tool".
 * Thus, we don’t put the focus on what this topology is actually doing&mdsash;the point is to have an example of a
 * typical topology that has input topics, intermediate topics, and output topics.
 * One important part in the code is the call to {@link KafkaStreams#cleanUp()}.
 * This call performs a local application (instance) reset and must be part in the code to make the application "reset ready".
 * <p>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper and Kafka. Please refer to <a href='http://docs.confluent.io/3.0.1/quickstart.html#quickstart'>CP3.0.1 QuickStart</a>.
 * <p>
 * 2) Create the input, intermediate, and output topics used by this example.
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic my-input-topic \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic rekeyed-topic \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic my-output-topic \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * }</pre>
 * Note: The above commands are for CP 3.0.1 only. For Apache Kafka it should be {@code bin/kafka-topics.sh ...}.
 * <p>
 * 3) Start this example application either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href='https://github.com/confluentinc/examples/tree/master/kafka-streams#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/streams-examples-3.0.1-standalone.jar io.confluent.examples.streams.ApplicationResetExample
 * }</pre>
 * 4) Write some input data to the source topic (e.g. via {@code kafka-console-producer}).
 * The already running example application (step 3) will automatically process this input data and write the results to the output topics.
 * <pre>
 * {@code
 * # Start the console producer. You can then enter input data by writing some line of text, followed by ENTER:
 * #
 * #    hello world<ENTER>
 * #    hello kafka streams<ENTER>
 * #    all streams lead to kafka<ENTER>
 * #
 * # Every line you enter will become the value of a single Kafka message.
 * $ bin/kafka-console-producer --broker-list localhost:9092 --topic my-input-topic
 * }</pre>
 * 5) Inspect the resulting data in the output topic, e.g. via {@code kafka-console-consumer}.
 * <pre>
 * {@code
 * $ bin/kafka-console-consumer --topic my-output-topic --from-beginning \
 *                              --zookeeper localhost:2181 \
 *                              --property print.key=true \
 *                              --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 * }</pre>
 * You should see output data similar to:
 * <pre>
 * {@code
 *     hello 1
 *     hello 2
 *     all 1
 * }</pre>
 * 6) Now you can stop the Streams application via {@code Ctrl-C}.
 * <p>
 * 7) If you would restart this application again (and not add any new data the the input topic),
 * the application would idle and wait for new data as it resumes where it was stopped.
 * If you want to reprocess the input data that is already contained in the input topic,
 * you first need to <strong>reset</strong> your application as follows:
 * <pre>
 * {@code
 * $ bin/kafka-streams-application-reset --application-id my-streams-app \
 *                                       --input-topics my-input-topic \
 *                                       --intermediate-topics rekeyed-topic
 * }</pre>
 * 8) You can now restart the application to reprocess the input topic from scratch.
 * For this, there is also a "local cleanup" required.
 * In this example application, you need to specify command line argument {@code --reset} to tell the application to
 * call {@link KafkaStreams#cleanUp()} before the application gets started.
 * Thus, restart the application via:
 * <pre>
 * {@code
 * $ java -cp target/streams-examples-3.0.1-standalone.jar io.confluent.examples.streams.ApplicationResetExample --reset
 * }</pre>
 * 9) If your console consumer (from step 5) is still running, you should see the same output data again.
 * If it was stopped and you restart it, if will print the result "twice".
 * Resetting an application does not modify output topics, thus, for this example, the output topic will contain the result twice.
 * <p>
 * 10) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}.
 * If needed, also stop the Kafka broker ({@code Ctrl-C}), and only then stop the ZooKeeper instance ({@code Ctrl-C}).
 */
public class ApplicationResetExample {

    public static void main(final String[] args) throws Exception {
        // Kafka Streams configuration
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-app");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // make sure to consume the complete topic via "auto.offset.reset = earliest"
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaStreams streams = run(args, streamsConfiguration);

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                streams.close();
            }
        }));
    }

    public static KafkaStreams run(final String[] args, final Properties streamsConfiguration) {
        // Define the processing topology
        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, String> input = builder.stream("my-input-topic");
        input.selectKey(
            new KeyValueMapper<String, String, String>() {
                @Override
                public String apply(final String key, final String value) {
                    return value.split(" ")[0];
                }
            })
            .through("rekeyed-topic")
            .countByKey("count")
            .to(Serdes.String(), Serdes.Long(), "my-output-topic");

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);

        // Delete the application's local state on reset
        if (args.length > 0 && args[0].equals("--reset")) {
            streams.cleanUp();
        }

        streams.start();

        return streams;
    }

}
