/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaMdc;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionType;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.extension.KafkaClusterConfigSupplier;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaSourceTest {
    private KafkaSource kafkaSource;

    @Mock
    private KafkaSourceConfig sourceConfig;

    @Mock
    private EncryptionConfig encryptionConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private SchemaConfig schemaConfig;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    TopicConsumerConfig topic1, topic2;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private KafkaClusterConfigSupplier kafkaClusterConfigSupplier;

    @Mock
    private PluginConfigObservable pluginConfigObservable;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    private static final String TEST_GROUP_ID = "testGroupId";
    private static final String TEST_CLIENT_ID = "testClientId";

    public KafkaSource createObjectUnderTest() {
        return new KafkaSource(
                sourceConfig, pluginMetrics, acknowledgementSetManager, pipelineDescription,
                kafkaClusterConfigSupplier, pluginConfigObservable, awsCredentialsSupplier);
    }

    @BeforeEach
    void setUp() throws Exception {
        sourceConfig = mock(KafkaSourceConfig.class);
        encryptionConfig = mock(EncryptionConfig.class);
        pipelineDescription = mock(PipelineDescription.class);
        pluginMetrics = mock(PluginMetrics.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        when(topic1.getName()).thenReturn("topic1");
        when(topic2.getName()).thenReturn("topic2");
        when(topic1.getWorkers()).thenReturn(2);
        when(topic2.getWorkers()).thenReturn(3);
        when(topic1.getCommitInterval()).thenReturn(Duration.ofSeconds(1));
        when(topic2.getCommitInterval()).thenReturn(Duration.ofSeconds(1));
        when(topic1.getAutoOffsetReset()).thenReturn("earliest");
        when(topic2.getAutoOffsetReset()).thenReturn("earliest");
        when(topic1.getConsumerMaxPollRecords()).thenReturn(1);
        when(topic2.getConsumerMaxPollRecords()).thenReturn(1);
        when(topic1.getGroupId()).thenReturn(TEST_GROUP_ID);
        when(topic2.getGroupId()).thenReturn(TEST_GROUP_ID);
        when(topic1.getClientId()).thenReturn(TEST_CLIENT_ID);
        when(topic2.getClientId()).thenReturn(TEST_CLIENT_ID);
        when(topic1.getMaxPollInterval()).thenReturn(Duration.ofSeconds(5));
        when(topic2.getMaxPollInterval()).thenReturn(Duration.ofSeconds(5));
        when(topic1.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(5));
        when(topic2.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(5));
        when(topic1.getAutoCommit()).thenReturn(false);
        when(topic1.getSerdeFormat()).thenReturn(MessageFormat.PLAINTEXT);
        when(topic2.getSerdeFormat()).thenReturn(MessageFormat.PLAINTEXT);
        when(topic2.getAutoCommit()).thenReturn(false);
        when(topic1.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(10));
        when(topic2.getThreadWaitingTime()).thenReturn(Duration.ofSeconds(10));
        when(sourceConfig.getBootstrapServers()).thenReturn(Collections.singletonList("http://localhost:1234"));
        when(sourceConfig.getTopics()).thenReturn((List) List.of(topic1, topic2));
        when(sourceConfig.getSchemaConfig()).thenReturn(null);
        when(sourceConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
        when(sourceConfig.getAcknowledgementsEnabled()).thenReturn(false);
        when(encryptionConfig.getType()).thenReturn(EncryptionType.NONE);
    }

   /* @Test
    void test_kafkaSource_start_stop() {
        kafkaSource = createObjectUnderTest();
        kafkaSource.start(buffer);
        try {
            Thread.sleep(10);
        } catch (Exception e){}
        kafkaSource.stop();
    }*/

    @Test
    void test_kafkaSource_start_execution_catch_block() {
        kafkaSource = createObjectUnderTest();
        Assertions.assertThrows(Exception.class, () -> kafkaSource.start(null));
    }

    @Test
    void test_kafkaSource_start_execution_exception() {
        kafkaSource = createObjectUnderTest();
        Assertions.assertThrows(Exception.class, () -> kafkaSource.start(buffer));
    }

    @Test
    void test_kafkaSource_basicFunctionality() {
        when(topic1.getSessionTimeOut()).thenReturn(Duration.ofSeconds(15));
        when(topic2.getSessionTimeOut()).thenReturn(Duration.ofSeconds(15));
        kafkaSource = createObjectUnderTest();
        assertTrue(Objects.nonNull(kafkaSource));
        kafkaSource.start(buffer);
        assertTrue(Objects.nonNull(kafkaSource.getConsumer()));
        assertFalse(kafkaSource.areAcknowledgementsEnabled());
    }

    @Test
    void test_kafkaSource_withAcknowledgements() {
        when(sourceConfig.getAcknowledgementsEnabled()).thenReturn(true);
        kafkaSource = createObjectUnderTest();
        assertTrue(Objects.nonNull(kafkaSource));
        assertTrue(kafkaSource.areAcknowledgementsEnabled());
    }

    @Test
    void test_kafkaSource_basicFunctionalityWithClientIdNull() {
        when(topic1.getSessionTimeOut()).thenReturn(Duration.ofSeconds(15));
        when(topic2.getSessionTimeOut()).thenReturn(Duration.ofSeconds(15));
        when(topic1.getClientId()).thenReturn(null);
        when(topic1.getClientId()).thenReturn(null);
        kafkaSource = createObjectUnderTest();
        assertTrue(Objects.nonNull(kafkaSource));
        kafkaSource.start(buffer);
        assertTrue(Objects.nonNull(kafkaSource.getConsumer()));
    }

    @Test
    void test_kafkaSource_retry_consumer_create() throws InterruptedException {
        when(topic1.getSessionTimeOut()).thenReturn(Duration.ofSeconds(15));
        when(topic2.getSessionTimeOut()).thenReturn(Duration.ofSeconds(15));
        kafkaSource = spy(createObjectUnderTest());
        doNothing().when(kafkaSource).sleep(anyLong());

        doThrow(new ConfigException("No resolvable bootstrap urls given in bootstrap.servers"))
            .doCallRealMethod()
            .when(kafkaSource)
            .createKafkaConsumer(any(), any());
        kafkaSource.start(buffer);
    }

    @Test
    void test_updateConfig_using_kafkaClusterConfigExtension() {
        final List<String> bootstrapServers = List.of("localhost:9092");
        final AuthConfig authConfig = mock(AuthConfig.class);
        final AwsConfig awsConfig = mock(AwsConfig.class);
        final EncryptionConfig encryptionConfig = mock(EncryptionConfig.class);
        doNothing().when(sourceConfig).setBootStrapServers(bootstrapServers);
        doNothing().when(sourceConfig).setAuthConfig(any());
        doNothing().when(sourceConfig).setAwsConfig(any());
        doNothing().when(sourceConfig).setEncryptionConfig(any());
        when(sourceConfig.getAuthConfig()).thenReturn(null);
        when(sourceConfig.getAwsConfig()).thenReturn(null);
        when(sourceConfig.getEncryptionConfigRaw()).thenReturn(null);
        when(sourceConfig.getBootstrapServers()).thenReturn(null);
        when(kafkaClusterConfigSupplier.getBootStrapServers()).thenReturn(bootstrapServers);
        when(kafkaClusterConfigSupplier.getAuthConfig()).thenReturn(authConfig);
        when(kafkaClusterConfigSupplier.getAwsConfig()).thenReturn(awsConfig);
        when(kafkaClusterConfigSupplier.getEncryptionConfig()).thenReturn(encryptionConfig);
        kafkaSource = createObjectUnderTest();
        verify(sourceConfig).setBootStrapServers(bootstrapServers);
        verify(sourceConfig).setAuthConfig(authConfig);
        verify(sourceConfig).setAwsConfig(awsConfig);
        verify(sourceConfig).setEncryptionConfig(encryptionConfig);
    }

    @Test
    void test_updateConfig_not_using_kafkaClusterConfigExtension() {
        final List<String> bootstrapServers = List.of("localhost:9092");
        final AuthConfig authConfig = mock(AuthConfig.class);
        final AwsConfig awsConfig = mock(AwsConfig.class);
        final EncryptionConfig encryptionConfig = mock(EncryptionConfig.class);
        doNothing().when(sourceConfig).setBootStrapServers(any());
        doNothing().when(sourceConfig).setAuthConfig(any());
        doNothing().when(sourceConfig).setAwsConfig(any());
        doNothing().when(sourceConfig).setEncryptionConfig(any());
        when(sourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(sourceConfig.getAwsConfig()).thenReturn(awsConfig);
        when(sourceConfig.getEncryptionConfigRaw()).thenReturn(encryptionConfig);
        when(sourceConfig.getBootstrapServers()).thenReturn(bootstrapServers);
        kafkaSource = createObjectUnderTest();
        verify(sourceConfig, never()).setBootStrapServers(any());
        verify(sourceConfig, never()).setAuthConfig(any());
        verify(sourceConfig, never()).setAwsConfig(any());
        verify(sourceConfig, never()).setEncryptionConfig(any());
    }

    @Nested
    class MdcTests {
        private MockedStatic<MDC> mdcMockedStatic;

        @BeforeEach
        void setUp() {
            mdcMockedStatic = mockStatic(MDC.class);
        }

        @AfterEach
        void tearDown() {
            mdcMockedStatic.close();
        }

        @Test
        void start_sets_and_removes_MDC() {
            when(topic1.getSessionTimeOut()).thenReturn(Duration.ofSeconds(15));
            when(topic2.getSessionTimeOut()).thenReturn(Duration.ofSeconds(15));

            createObjectUnderTest().start(buffer);

            mdcMockedStatic.verify(() -> MDC.put(KafkaMdc.MDC_KAFKA_PLUGIN_KEY, "source"));
            mdcMockedStatic.verify(() -> MDC.remove(KafkaMdc.MDC_KAFKA_PLUGIN_KEY));
        }

        @Test
        void stop_sets_and_removes_MDC() {
            createObjectUnderTest().stop();

            mdcMockedStatic.verify(() -> MDC.put(KafkaMdc.MDC_KAFKA_PLUGIN_KEY, "source"));
            mdcMockedStatic.verify(() -> MDC.remove(KafkaMdc.MDC_KAFKA_PLUGIN_KEY));
        }
    }
}
