/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.kafka.AuthConfig;
import org.opensearch.dataprepper.model.plugin.kafka.AwsConfig;
import org.opensearch.dataprepper.model.plugin.kafka.EncryptionConfig;
import org.opensearch.dataprepper.model.plugin.kafka.EncryptionType;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaSourceTest {
    private static final String TEST_GROUP_ID = "testGroupId";
    @Mock
    TopicConfig topic1, topic2;
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
    private Buffer<Record<Event>> buffer;

    public KafkaSource createObjectUnderTest() {
        return new KafkaSource(sourceConfig, pluginMetrics, acknowledgementSetManager, pipelineDescription);
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
        when(sourceConfig.getBootStrapServers()).thenReturn("http://localhost:1234");
        when(sourceConfig.getTopics()).thenReturn(Arrays.asList(topic1, topic2));
        when(sourceConfig.getSchemaConfig()).thenReturn(null);
        when(sourceConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
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
    }

    @Test
    void test_setBootstrapServers() {
        kafkaSource = createObjectUnderTest();
        final List<String> bootstrapServers = List.of("localhost:9092");
        doNothing().when(sourceConfig).setBootStrapServers(bootstrapServers);
        // should not set bootstrap if has existing value
        when(sourceConfig.getBootStrapServers()).thenReturn("existing:9092");
        kafkaSource.setBootstrapServers(bootstrapServers);
        verify(sourceConfig, never()).setBootStrapServers(bootstrapServers);
        // should set bootstrap if no existing value
        when(sourceConfig.getBootStrapServers()).thenReturn(null);
        kafkaSource.setBootstrapServers(bootstrapServers);
        verify(sourceConfig).setBootStrapServers(bootstrapServers);
    }

    @Test
    void test_setKafkaClusterAuthConfig() {
        kafkaSource = createObjectUnderTest();
        AuthConfig authConfig = mock(AuthConfig.class);
        AwsConfig awsConfig = mock(AwsConfig.class);
        EncryptionConfig encryptionConfig = mock(EncryptionConfig.class);
        doNothing().when(sourceConfig).setAuthConfig(any());
        doNothing().when(sourceConfig).setAwsConfig(any());
        doNothing().when(sourceConfig).setEncryptionConfig(any());
        // should not set bootstrap if has existing value
        when(sourceConfig.getAuthConfig()).thenReturn(authConfig);
        when(sourceConfig.getAwsConfig()).thenReturn(awsConfig);
        when(sourceConfig.getEncryptionConfigRaw()).thenReturn(encryptionConfig);
        kafkaSource.setKafkaClusterAuthConfig(authConfig, awsConfig, encryptionConfig);
        verify(sourceConfig, never()).setAuthConfig(any());
        verify(sourceConfig, never()).setAwsConfig(any());
        verify(sourceConfig, never()).setEncryptionConfig(encryptionConfig);
        // should set bootstrap if no existing value
        when(sourceConfig.getAuthConfig()).thenReturn(null);
        when(sourceConfig.getAwsConfig()).thenReturn(null);
        when(sourceConfig.getEncryptionConfigRaw()).thenReturn(null);
        kafkaSource.setKafkaClusterAuthConfig(authConfig, awsConfig, encryptionConfig);
        verify(sourceConfig).setAuthConfig(any());
        verify(sourceConfig).setAwsConfig(any());
        verify(sourceConfig).setEncryptionConfig(encryptionConfig);
    }
}
