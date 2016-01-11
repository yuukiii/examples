package io.confluent.examples.streams.classes;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Collection;
import java.util.Map;

public class CollectionSerializer<T> implements Serializer<Collection<T>> {

    /**
     * Constructor used by Kafka Streams.
     */
    public CollectionSerializer() {

    }
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public byte[] serialize(String topic, Collection<T> record) {
        // placeholder that will not work
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
