package io.confluent.examples.streams.queryablestate;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import io.confluent.examples.streams.IntegrationTestUtils;

public class QueryableStateProducer {


  @Test
  public void shouldDemonstrateQueryableState() throws Exception {
    final List<String> inputValues = Arrays.asList("hello world",
                                                   "all streams lead to kafka",
                                                   "streams",
                                                   "kafka streams",
                                                   "the cat in the hat",
                                                   "green eggs and ham",
                                                   "that sam i am",
                                                   "up the creek without a paddle",
                                                   "run forest run",
                                                   "a tank full of gas",
                                                   "eat sleep rave repeat",
                                                   "one jolly sailor",
                                                   "king of the world");

    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    final KafkaProducer<String, String>
        producer =
        new KafkaProducer<>(producerConfig, new StringSerializer(), new StringSerializer());

    final Random random = new Random();
    while (true) {
      final int i = random.nextInt(inputValues.size());
      producer.send(new ProducerRecord<>(QueryableStateExample.TEXT_LINES_TOPIC,
                                                       inputValues.get(i), inputValues.get(i)));
      Thread.sleep(500L);
    }
  }

}