/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.codec.JsonDecoder;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.util.TestConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KafkaBufferIT {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBufferIT.class);
    @Mock
    private PluginSetting pluginSetting;

    private KafkaBufferConfig kafkaBufferConfig;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    private BufferTopicConfig topicConfig;

    private ByteDecoder byteDecoder;

    private String bootstrapServersCommaDelimited;
    private String topicName;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

        when(pluginSetting.getPipelineName()).thenReturn(UUID.randomUUID().toString());

        topicName = "buffer-" + RandomStringUtils.randomAlphabetic(5);

        final Map<String, Object> topicConfigMap = Map.of(
                "name", topicName,
                "group_id", "buffergroup-" + RandomStringUtils.randomAlphabetic(6),
                "create_topic", true
        );

        topicConfig = objectMapper.convertValue(topicConfigMap, BufferTopicConfig.class);

        bootstrapServersCommaDelimited = System.getProperty("tests.kafka.bootstrap_servers");

        LOG.info("Using Kafka bootstrap servers: {}", bootstrapServersCommaDelimited);

        final Map<String, Object> bufferConfigMap = Map.of(
                "topics", List.of(topicConfigMap),
                "bootstrap_servers", List.of(bootstrapServersCommaDelimited),
                "encryption", Map.of("type", "none")
        );
        kafkaBufferConfig = objectMapper.convertValue(bufferConfigMap, KafkaBufferConfig.class);

        byteDecoder = null;
    }

    private KafkaBuffer createObjectUnderTest() {
        return new KafkaBuffer(pluginSetting, kafkaBufferConfig, pluginFactory, acknowledgementSetManager, null, null, null);
    }

    @Test
    void write_and_read() throws TimeoutException {
        KafkaBuffer objectUnderTest = createObjectUnderTest();

        Record<Event> record = createRecord();
        objectUnderTest.write(record, 1_000);

        Map.Entry<Collection<Record<Event>>, CheckpointState> readResult = objectUnderTest.read(10_000);

        assertThat(readResult, notNullValue());
        assertThat(readResult.getKey(), notNullValue());
        assertThat(readResult.getKey().size(), equalTo(1));

        Record<Event> onlyResult = readResult.getKey().stream().iterator().next();

        assertThat(onlyResult, notNullValue());
        assertThat(onlyResult.getData(), notNullValue());
        // TODO: The metadata is not included. It needs to be included in the Buffer, though not in the Sink. This may be something we make configurable in the consumer/producer - whether to serialize the metadata or not.
        //assertThat(onlyResult.getData().getMetadata(), equalTo(record.getData().getMetadata()));
        assertThat(onlyResult.getData().toMap(), equalTo(record.getData().toMap()));
    }

    @Test
    void write_and_read_bytes() throws Exception {
        byteDecoder = new JsonDecoder();

        KafkaBuffer objectUnderTest = createObjectUnderTest();

        Map<String, String> map = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());
        byte[] bytes = objectMapper.writeValueAsBytes(map);
        String key = UUID.randomUUID().toString();
        objectUnderTest.writeBytes(bytes, key, 1_000);

        Map.Entry<Collection<Record<Event>>, CheckpointState> readResult = objectUnderTest.read(10_000);

        assertThat(readResult, notNullValue());
        assertThat(readResult.getKey(), notNullValue());
        assertThat(readResult.getKey().size(), equalTo(1));

        Record<Event> onlyResult = readResult.getKey().stream().iterator().next();

        assertThat(onlyResult, notNullValue());
        assertThat(onlyResult.getData(), notNullValue());
        // TODO: The metadata is not included. It needs to be included in the Buffer, though not in the Sink. This may be something we make configurable in the consumer/producer - whether to serialize the metadata or not.
        //assertThat(onlyResult.getData().getMetadata(), equalTo(record.getData().getMetadata()));
        //assertThat(onlyResult.getData().toMap(), equalTo(record.getData().toMap()));
    }

    @Test
    void write_data_is_correctly_formatted_in_protobuf_wrapper() throws TimeoutException, IOException {
        final KafkaBuffer objectUnderTest = createObjectUnderTest();

        final Record<Event> record = createRecord();

        final TestConsumer testConsumer = new TestConsumer(bootstrapServersCommaDelimited, topicName);

        objectUnderTest.write(record, 1_000);

        final List<ConsumerRecord<byte[], byte[]>> consumerRecords = new ArrayList<>();

        await().atMost(Duration.ofSeconds(10))
                .until(() -> {
                    testConsumer.readConsumerRecords(consumerRecords);
                    return !consumerRecords.isEmpty();
                });

        assertThat(consumerRecords.size(), equalTo(1));

        final ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecords.get(0);

        assertThat(consumerRecord, notNullValue());
        assertThat(consumerRecord.value(), notNullValue());

        final KafkaBufferMessage.BufferData bufferData = KafkaBufferMessage.BufferData.parseFrom(consumerRecord.value());

        assertThat(bufferData, notNullValue());
        final byte[] innerData = bufferData.getData().toByteArray();

        final Map<String, Object> actualEventData = objectMapper.readValue(innerData, Map.class);
        assertThat(actualEventData, notNullValue());
        assertThat(actualEventData, hasKey("message"));
        assertThat(actualEventData.get("message"), equalTo(record.getData().get("message", String.class)));
    }

    @Nested
    class Encrypted {
        private Cipher cipher;

        @BeforeEach
        void setUp() throws NoSuchAlgorithmException, InvalidKeyException, NoSuchPaddingException {
            final KeyGenerator aesKeyGenerator = KeyGenerator.getInstance("AES");
            aesKeyGenerator.init(256);
            final SecretKey secretKey = aesKeyGenerator.generateKey();

            cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            final byte[] base64Bytes = Base64.getEncoder().encode(secretKey.getEncoded());
            final String aesKey = new String(base64Bytes);

            final Map<String, Object> topicConfigMap = objectMapper.convertValue(topicConfig, Map.class);
            topicConfigMap.put("encryption_key", aesKey);
            final Map<String, Object> bufferConfigMap = objectMapper.convertValue(kafkaBufferConfig, Map.class);
            bufferConfigMap.put("topics", List.of(topicConfigMap));
            kafkaBufferConfig = objectMapper.convertValue(bufferConfigMap, KafkaBufferConfig.class);
        }

        @Test
        void write_and_read_encrypted() throws TimeoutException {
            KafkaBuffer objectUnderTest = createObjectUnderTest();

            Record<Event> record = createRecord();
            objectUnderTest.write(record, 1_000);

            Map.Entry<Collection<Record<Event>>, CheckpointState> readResult = objectUnderTest.read(10_000);

            assertThat(readResult, notNullValue());
            assertThat(readResult.getKey(), notNullValue());
            assertThat(readResult.getKey().size(), equalTo(1));

            Record<Event> onlyResult = readResult.getKey().stream().iterator().next();

            assertThat(onlyResult, notNullValue());
            assertThat(onlyResult.getData(), notNullValue());
            // TODO: The metadata is not included. It needs to be included in the Buffer, though not in the Sink. This may be something we make configurable in the consumer/producer - whether to serialize the metadata or not.
            //assertThat(onlyResult.getData().getMetadata(), equalTo(record.getData().getMetadata()));
            assertThat(onlyResult.getData().toMap(), equalTo(record.getData().toMap()));
        }

        @Test
        void write_data_is_correctly_formatted_encrypted() throws TimeoutException, IOException, IllegalBlockSizeException, BadPaddingException {
            final KafkaBuffer objectUnderTest = createObjectUnderTest();

            final Record<Event> record = createRecord();

            final TestConsumer testConsumer = new TestConsumer(bootstrapServersCommaDelimited, topicName);

            objectUnderTest.write(record, 1_000);

            final List<ConsumerRecord<byte[], byte[]>> consumerRecords = new ArrayList<>();

            await().atMost(Duration.ofSeconds(10))
                    .until(() -> {
                        testConsumer.readConsumerRecords(consumerRecords);
                        return !consumerRecords.isEmpty();
                    });

            assertThat(consumerRecords.size(), equalTo(1));

            final ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecords.get(0);

            assertThat(consumerRecord, notNullValue());
            final byte[] valueBytes = consumerRecord.value();
            assertThat(valueBytes, notNullValue());

            final KafkaBufferMessage.BufferData bufferData = KafkaBufferMessage.BufferData.parseFrom(valueBytes);

            assertThat(bufferData, notNullValue());
            byte[] innerData = bufferData.getData().toByteArray();

            assertThat(innerData, notNullValue());
            assertThrows(JsonParseException.class, () -> objectMapper.readValue(innerData, Map.class));

            final byte[] deserializedBytes = cipher.doFinal(innerData);

            final Map<String, Object> actualEventData = objectMapper.readValue(deserializedBytes, Map.class);
            assertThat(actualEventData, notNullValue());
            assertThat(actualEventData, hasKey("message"));
            assertThat(actualEventData.get("message"), equalTo(record.getData().get("message", String.class)));
        }
    }

    private Record<Event> createRecord() {
        Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        return new Record<>(event);
    }
}
