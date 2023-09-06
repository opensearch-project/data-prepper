/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;
import org.opensearch.dataprepper.plugins.kafka.extension.KafkaClusterConfigSupplier;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSourceSecurityConfigurer;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.MySQLConfig;
import org.opensearch.dataprepper.plugins.kafkaconnect.extension.KafkaConnectConfig;
import org.opensearch.dataprepper.plugins.kafkaconnect.extension.KafkaConnectConfigSupplier;
import org.opensearch.dataprepper.plugins.kafkaconnect.extension.WorkerProperties;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.KafkaConnect;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KafkaConnectSourceTest {
    private final String TEST_PIPELINE_NAME = "test_pipeline";
    private KafkaConnectSource kafkaConnectSource;

    @Mock
    private MySQLConfig mySQLConfig;

    @Mock
    private KafkaConnectConfig kafkaConnectConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    private Buffer<Record<Object>> buffer;

    @Mock
    private KafkaConnect kafkaConnect;

    @Mock
    private KafkaClusterConfigSupplier kafkaClusterConfigSupplier;

    @Mock
    private KafkaConnectConfigSupplier kafkaConnectConfigSupplier;

    private String bootstrapServers = "localhost:9092";

    public KafkaConnectSource createSourceUnderTest() {
        return new MySQLSource(mySQLConfig, pluginMetrics, pipelineDescription, kafkaClusterConfigSupplier, kafkaConnectConfigSupplier);
    }

    @BeforeEach
    void setUp() {
        WorkerProperties workerProperties = new WorkerProperties();
        workerProperties.setBootstrapServers(bootstrapServers);
        kafkaConnectConfigSupplier = mock(KafkaConnectConfigSupplier.class);
        lenient().when(kafkaConnectConfigSupplier.getConfig()).thenReturn(kafkaConnectConfig);
        lenient().when(kafkaConnectConfig.getWorkerProperties()).thenReturn(workerProperties);
        lenient().when(mySQLConfig.buildConnectors()).thenReturn(Collections.emptyList());

        pipelineDescription = mock(PipelineDescription.class);
        lenient().when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        pluginMetrics = mock(PluginMetrics.class);
    }

    @Test
    void testStartKafkaConnectSource() {
        try (MockedStatic<KafkaConnect> mockedStatic = mockStatic(KafkaConnect.class);
             MockedStatic<KafkaSourceSecurityConfigurer> mockedSecurityConfigurer = mockStatic(KafkaSourceSecurityConfigurer.class)) {
            mockedSecurityConfigurer.when(() -> KafkaSourceSecurityConfigurer.setAuthProperties(any(), any(), any())).thenAnswer((Answer<Void>) invocation -> null);
            kafkaConnect = mock(KafkaConnect.class);
            doNothing().when(kafkaConnect).addConnectors(any());
            doNothing().when(kafkaConnect).start();
            doNothing().when(kafkaConnect).stop();
            // Set up the mock behavior for the static method getInstance()
            mockedStatic.when(() -> KafkaConnect.getPipelineInstance(any(), any(), anyLong(), anyLong())).thenReturn(kafkaConnect);
            kafkaConnectSource = createSourceUnderTest();
            kafkaConnectSource.start(buffer);
            verify(kafkaConnect).addConnectors(any());
            verify(kafkaConnect).start();
            try {
                Thread.sleep(10);
            } catch (Exception e) {
                e.printStackTrace();
            }
            kafkaConnectSource.stop();
            verify(kafkaConnect).start();
        }
    }

    @Test
    void testStartKafkaConnectSourceError() {
        WorkerProperties workerProperties = new WorkerProperties();
        workerProperties.setBootstrapServers(null);
        lenient().when(kafkaConnectConfig.getWorkerProperties()).thenReturn(workerProperties);
        try (MockedStatic<KafkaConnect> mockedStatic = mockStatic(KafkaConnect.class);
             MockedStatic<KafkaSourceSecurityConfigurer> mockedSecurityConfigurer = mockStatic(KafkaSourceSecurityConfigurer.class)) {
            mockedSecurityConfigurer.when(() -> KafkaSourceSecurityConfigurer.setAuthProperties(any(), any(), any())).thenAnswer((Answer<Void>) invocation -> null);
            kafkaConnect = mock(KafkaConnect.class);
            // Set up the mock behavior for the static method getInstance()
            mockedStatic.when(() -> KafkaConnect.getPipelineInstance(any(), any(), anyLong(), anyLong())).thenReturn(kafkaConnect);
            kafkaConnectSource = createSourceUnderTest();
            assertThrows(IllegalArgumentException.class, () -> kafkaConnectSource.start(buffer));
        }
    }

    @Test
    void test_updateConfig_using_kafkaClusterConfigExtension() {
        final List<String> bootstrapServers = List.of("localhost:9092");
        final AuthConfig authConfig = mock(AuthConfig.class);
        final AwsConfig awsConfig = mock(AwsConfig.class);
        final EncryptionConfig encryptionConfig = mock(EncryptionConfig.class);
        doNothing().when(kafkaConnectConfig).setBootstrapServers(any());
        doNothing().when(kafkaConnectConfig).setAuthConfig(any());
        doNothing().when(kafkaConnectConfig).setAwsConfig(any());
        doNothing().when(kafkaConnectConfig).setEncryptionConfig(any());
        when(kafkaConnectConfig.getAuthConfig()).thenReturn(null);
        when(kafkaConnectConfig.getAwsConfig()).thenReturn(null);
        when(kafkaConnectConfig.getEncryptionConfig()).thenReturn(null);
        when(kafkaConnectConfig.getBootStrapServers()).thenReturn(null);
        when(kafkaClusterConfigSupplier.getBootStrapServers()).thenReturn(bootstrapServers);
        when(kafkaClusterConfigSupplier.getAuthConfig()).thenReturn(authConfig);
        when(kafkaClusterConfigSupplier.getAwsConfig()).thenReturn(awsConfig);
        when(kafkaClusterConfigSupplier.getEncryptionConfig()).thenReturn(encryptionConfig);
        try (MockedStatic<KafkaSourceSecurityConfigurer> mockedStatic = mockStatic(KafkaSourceSecurityConfigurer.class)) {
            mockedStatic.when(() -> KafkaSourceSecurityConfigurer.setAuthProperties(any(), any(), any())).thenAnswer((Answer<Void>) invocation -> null);
            kafkaConnectSource = createSourceUnderTest();
            verify(kafkaConnectConfig).setBootstrapServers(bootstrapServers);
            verify(kafkaConnectConfig).setAuthConfig(authConfig);
            verify(kafkaConnectConfig).setAwsConfig(awsConfig);
            verify(kafkaConnectConfig).setEncryptionConfig(encryptionConfig);
        }
    }

    @Test
    void test_updateConfig_not_using_kafkaClusterConfigExtension() {
        final List<String> bootstrapServers = List.of("localhost:9092");
        final AuthConfig authConfig = mock(AuthConfig.class);
        final AwsConfig awsConfig = mock(AwsConfig.class);
        final EncryptionConfig encryptionConfig = mock(EncryptionConfig.class);
        lenient().doNothing().when(kafkaConnectConfig).setBootstrapServers(any());
        lenient().doNothing().when(kafkaConnectConfig).setAuthConfig(any());
        lenient().doNothing().when(kafkaConnectConfig).setAwsConfig(any());
        lenient().doNothing().when(kafkaConnectConfig).setEncryptionConfig(any());
        lenient().when(kafkaConnectConfig.getAuthConfig()).thenReturn(authConfig);
        lenient().when(kafkaConnectConfig.getAwsConfig()).thenReturn(awsConfig);
        lenient().when(kafkaConnectConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
        lenient().when(kafkaConnectConfig.getBootStrapServers()).thenReturn(String.join(",", bootstrapServers));
        try (MockedStatic<KafkaSourceSecurityConfigurer> mockedStatic = mockStatic(KafkaSourceSecurityConfigurer.class)) {
            mockedStatic.when(() -> KafkaSourceSecurityConfigurer.setAuthProperties(any(), any(), any())).thenAnswer((Answer<Void>) invocation -> null);
            kafkaConnectSource = createSourceUnderTest();
            verify(kafkaConnectConfig, never()).setBootstrapServers(any());
            verify(kafkaConnectConfig, never()).setAuthConfig(any());
            verify(kafkaConnectConfig, never()).setAwsConfig(any());
            verify(kafkaConnectConfig, never()).setEncryptionConfig(any());
        }
    }
}
