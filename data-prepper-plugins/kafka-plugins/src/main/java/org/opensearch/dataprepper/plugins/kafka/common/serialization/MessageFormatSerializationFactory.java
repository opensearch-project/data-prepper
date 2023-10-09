package org.opensearch.dataprepper.plugins.kafka.common.serialization;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Provides the correct Kafka {@link Serializer} or {@link Deserializer} for a given {@link MessageFormat}.
 */
class MessageFormatSerializationFactory {
    public static final Class<StringDeserializer> DEFAULT_DESERIALIZER = StringDeserializer.class;
    public static final Class<StringSerializer> DEFAULT_SERIALIZER = StringSerializer.class;
    private final SerializationCache<Class<? extends Serializer<?>>, Serializer<?>> serializerCache = new SerializationCache<>();
    private final SerializationCache<Class<? extends Deserializer<?>>, Deserializer<?>> deserializerCache = new SerializationCache<>();

    private final Map<MessageFormat, Class<? extends Deserializer<?>>> deserializerClasses = Map.of(
            MessageFormat.PLAINTEXT, StringDeserializer.class,
            MessageFormat.BYTES, ByteArrayDeserializer.class,
            MessageFormat.JSON, JsonDeserializer.class,
            MessageFormat.AVRO, KafkaAvroDeserializer.class
    );

    private final Map<MessageFormat, Class<? extends Serializer<?>>> serializerClasses = Map.of(
            MessageFormat.PLAINTEXT, StringSerializer.class,
            MessageFormat.BYTES, ByteArraySerializer.class,
            MessageFormat.JSON, JsonSerializer.class,
            MessageFormat.AVRO, KafkaAvroSerializer.class
    );

    Deserializer<?> getDeserializer(MessageFormat messageFormat) {
        return genericGet(messageFormat, deserializerClasses, deserializerCache, DEFAULT_DESERIALIZER);
    }

    Serializer<?> getSerializer(MessageFormat messageFormat) {
        return genericGet(messageFormat, serializerClasses, serializerCache, DEFAULT_SERIALIZER);
    }

    private static <S, C extends Class<? extends S>> S genericGet(
            MessageFormat messageFormat,
            Map<MessageFormat, C> classMap,
            SerializationCache<C, S> cache,
            C defaultClass) {
        C serializerClass = classMap.getOrDefault(messageFormat, defaultClass);

        return cache.get(serializerClass, createDefaultConstructorSupplier(serializerClass));
    }

    private static <T> Supplier<T> createDefaultConstructorSupplier(Class<? extends T> clazz) {
        return () -> {
            try {
                return clazz.getConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                     NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static class SerializationCache<K, V> {
        private final Map<K, V> cache = new HashMap<>();

        V get(K key, Supplier<V> serializerCreator) {
            return cache.computeIfAbsent(key, k -> serializerCreator.get());
        }
    }
}
