package io.confluent.examples.streams.classes;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class CollectionDeserializer<T> implements Deserializer<Collection<T>> {

    /**
     * Constructor used by Kafka Streams.
     */
    public CollectionDeserializer() {

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public Collection<T> deserialize(String s, byte[] bytes) {
        // placeholder that will not work
        return Collections.<T>emptyList();
    }

    @Override
    public void close() {

    }
}
