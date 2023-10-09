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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class MessageFormatSerializationFactoryTest {
    private MessageFormatSerializationFactory createObjectUnderTest() {
        return new MessageFormatSerializationFactory();
    }

    @Test
    void getDeserializer_with_null_throws() {
        MessageFormatSerializationFactory objectUnderTest = createObjectUnderTest();
        assertThrows(NullPointerException.class, () -> objectUnderTest.getDeserializer(null));
    }

    @ParameterizedTest
    @ArgumentsSource(MessageFormatToExpectedDeserializerClassType.class)
    void getDeserializer_returns_expected_deserializer(MessageFormat messageFormat, Class<?> expectedClass) {
        assertThat(createObjectUnderTest().getDeserializer(messageFormat),
                instanceOf(expectedClass));
    }

    @ParameterizedTest
    @EnumSource(MessageFormat.class)
    void getDeserializer_returns_deserializer_instance(MessageFormat messageFormat) {
        assertThat(createObjectUnderTest().getDeserializer(messageFormat),
                instanceOf(Deserializer.class));
    }

    @ParameterizedTest
    @EnumSource(MessageFormat.class)
    void getDeserializer_returns_same_instance_for_multiple_calls(MessageFormat messageFormat) {
        MessageFormatSerializationFactory objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.getDeserializer(messageFormat),
                sameInstance(objectUnderTest.getDeserializer(messageFormat)));
    }

    @Test
    void getSerializer_with_null_throws() {
        MessageFormatSerializationFactory objectUnderTest = createObjectUnderTest();
        assertThrows(NullPointerException.class, () -> objectUnderTest.getSerializer(null));
    }

    @ParameterizedTest
    @ArgumentsSource(MessageFormatToExpectedSerializerClassType.class)
    void getSerializer_returns_expected_deserializer(MessageFormat messageFormat, Class<?> expectedClass) {
        assertThat(createObjectUnderTest().getSerializer(messageFormat),
                instanceOf(expectedClass));
    }

    @ParameterizedTest
    @EnumSource(MessageFormat.class)
    void getSerializer_returns_deserializer_instance(MessageFormat messageFormat) {
        assertThat(createObjectUnderTest().getSerializer(messageFormat),
                instanceOf(Serializer.class));
    }

    @ParameterizedTest
    @EnumSource(MessageFormat.class)
    void getSerializer_returns_same_instance_for_multiple_calls(MessageFormat messageFormat) {
        MessageFormatSerializationFactory objectUnderTest = createObjectUnderTest();
        assertThat(objectUnderTest.getSerializer(messageFormat),
                sameInstance(objectUnderTest.getSerializer(messageFormat)));
    }

    static class MessageFormatToExpectedDeserializerClassType implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    arguments(MessageFormat.BYTES, ByteArrayDeserializer.class),
                    arguments(MessageFormat.PLAINTEXT, StringDeserializer.class),
                    arguments(MessageFormat.JSON, JsonDeserializer.class),
                    arguments(MessageFormat.AVRO, KafkaAvroDeserializer.class)
            );
        }
    }

    static class MessageFormatToExpectedSerializerClassType implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    arguments(MessageFormat.BYTES, ByteArraySerializer.class),
                    arguments(MessageFormat.PLAINTEXT, StringSerializer.class),
                    arguments(MessageFormat.JSON, JsonSerializer.class),
                    arguments(MessageFormat.AVRO, KafkaAvroSerializer.class)
            );
        }
    }
}