/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.util.TestProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KafkaBuffer_KmsIT {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBuffer_KmsIT.class);
    @Mock
    private PluginSetting pluginSetting;

    private KafkaBufferConfig kafkaBufferConfig;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private AcknowledgementSet acknowledgementSet;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    private Random random;

    private BufferTopicConfig topicConfig;

    private ByteDecoder byteDecoder;

    private String bootstrapServersCommaDelimited;
    private String topicName;
    private ObjectMapper objectMapper;
    private String kmsKey;
    private KmsClient kmsClient;

    @BeforeEach
    void setUp() {
        System.setProperty("software.amazon.awssdk.http.service.impl", "software.amazon.awssdk.http.apache.ApacheSdkHttpService");
        random = new Random();
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        acknowledgementSet = mock(AcknowledgementSet.class);
        lenient().doAnswer((a) -> null).when(acknowledgementSet).complete();
        lenient().when(acknowledgementSetManager.create(any(), any(Duration.class))).thenReturn(acknowledgementSet);
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

        kmsKey = System.getProperty("tests.kafka.kms_key");
        kmsClient = KmsClient.create();

        byteDecoder = null;

        when(awsCredentialsSupplier.getProvider(any(AwsCredentialsOptions.class))).thenAnswer(options -> DefaultCredentialsProvider.create());
    }

    private KafkaBuffer createObjectUnderTest() {
        return new KafkaBuffer(pluginSetting, kafkaBufferConfig, acknowledgementSetManager, null, awsCredentialsSupplier, null);
    }

    @Nested
    class Encrypted {
        private Cipher encryptCipher;
        private String aesKey;

        @BeforeEach
        void setUp() throws NoSuchAlgorithmException, InvalidKeyException, NoSuchPaddingException {
            final GenerateDataKeyRequest request = GenerateDataKeyRequest
                    .builder()
                    .keyId(kmsKey)
                    .keySpec(DataKeySpec.AES_256)
                    .build();

            final GenerateDataKeyResponse response = kmsClient.generateDataKey(request);

            byte[] plaintextDataKey = response.plaintext().asByteArray();
            byte[] encryptedDataKey = response.ciphertextBlob().asByteArray();
            aesKey = Base64.getEncoder()
                    .withoutPadding()
                    .encodeToString(encryptedDataKey);

            final SecretKey secretKey = new SecretKeySpec(plaintextDataKey, 0, plaintextDataKey.length, "AES");
            encryptCipher = Cipher.getInstance("AES");
            encryptCipher.init(Cipher.ENCRYPT_MODE, secretKey);


            final Map<String, Object> topicConfigMap = objectMapper.convertValue(topicConfig, Map.class);
            topicConfigMap.put("encryption_key", aesKey);
            topicConfigMap.put("kms", Map.of(
                    "key_id", kmsKey
            ));
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
        void read_decrypts_data_from_the_predefined_key() throws IllegalBlockSizeException, BadPaddingException {
            final KafkaBuffer objectUnderTest = createObjectUnderTest();
            final TestProducer testProducer = new TestProducer(bootstrapServersCommaDelimited, topicName);

            final Record<Event> record = createRecord();
            final byte[] unencryptedBytes = record.getData().toJsonString().getBytes();
            final byte[] encryptedBytes = encryptBytes(unencryptedBytes);

            final KafkaBufferMessage.BufferData bufferedData = KafkaBufferMessage.BufferData.newBuilder()
                    .setMessageFormat(KafkaBufferMessage.MessageFormat.MESSAGE_FORMAT_BYTES)
                    .setData(ByteString.copyFrom(encryptedBytes))
                    .build();

            final byte[] unencryptedKeyBytes = createRandomBytes();
            final byte[] encryptedKeyBytes = encryptBytes(unencryptedKeyBytes);

            final KafkaBufferMessage.BufferData keyData = KafkaBufferMessage.BufferData.newBuilder()
                    .setMessageFormat(KafkaBufferMessage.MessageFormat.MESSAGE_FORMAT_BYTES)
                    .setData(ByteString.copyFrom(encryptedKeyBytes))
                    .build();

            testProducer.publishRecord(keyData.toByteArray(), bufferedData.toByteArray());

            final Map.Entry<Collection<Record<Event>>, CheckpointState> readResult = objectUnderTest.read(10_000);

            assertThat(readResult, notNullValue());
            assertThat(readResult.getKey(), notNullValue());
            assertThat(readResult.getKey().size(), equalTo(1));

            final Record<Event> onlyResult = readResult.getKey().stream().iterator().next();

            assertThat(onlyResult, notNullValue());
            assertThat(onlyResult.getData(), notNullValue());
            assertThat(onlyResult.getData().toMap(), equalTo(record.getData().toMap()));
        }

        private byte[] encryptBytes(final byte[] plaintextBytes) throws IllegalBlockSizeException, BadPaddingException {
            return encryptCipher.doFinal(plaintextBytes);
        }
    }

    private byte[] createRandomBytes() {
        final byte[] writtenBytes = new byte[128];
        random.nextBytes(writtenBytes);
        return writtenBytes;
    }

    private Record<Event> createRecord() {
        Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        return new Record<>(event);
    }

}
