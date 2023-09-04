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
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.KafkaConnectSourceConfig;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.WorkerProperties;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.KafkaConnect;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
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
    private KafkaConnectSourceConfig sourceConfig;

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

    private String bootstrapServers = "localhost:9092";

    public KafkaConnectSource createSourceUnderTest() {
        return new KafkaConnectSource(sourceConfig, pluginMetrics, pipelineDescription, kafkaClusterConfigSupplier);
    }

    @BeforeEach
    void setUp() throws Exception {
        WorkerProperties workerProperties = new WorkerProperties();
        workerProperties.setBootstrapServers(bootstrapServers);
        sourceConfig = mock(KafkaConnectSourceConfig.class);
        lenient().when(sourceConfig.getWorkerProperties()).thenReturn(workerProperties);
        lenient().when(sourceConfig.getConnectors()).thenReturn(Collections.emptyList());

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
            mockedStatic.when(() -> KafkaConnect.getPipelineInstance(any(), any())).thenReturn(kafkaConnect);
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
        lenient().when(sourceConfig.getWorkerProperties()).thenReturn(workerProperties);
        try (MockedStatic<KafkaConnect> mockedStatic = mockStatic(KafkaConnect.class);
             MockedStatic<KafkaSourceSecurityConfigurer> mockedSecurityConfigurer = mockStatic(KafkaSourceSecurityConfigurer.class)) {
            mockedSecurityConfigurer.when(() -> KafkaSourceSecurityConfigurer.setAuthProperties(any(), any(), any())).thenAnswer((Answer<Void>) invocation -> null);
            kafkaConnect = mock(KafkaConnect.class);
            // Set up the mock behavior for the static method getInstance()
            mockedStatic.when(() -> KafkaConnect.getPipelineInstance(any(), any())).thenReturn(kafkaConnect);
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
        doNothing().when(sourceConfig).setBootstrapServers(any());
        doNothing().when(sourceConfig).setAuthConfig(any());
        doNothing().when(sourceConfig).setAwsConfig(any());
        doNothing().when(sourceConfig).setEncryptionConfig(any());
        when(sourceConfig.getAuthConfig()).thenReturn(null);
        when(sourceConfig.getAwsConfig()).thenReturn(null);
        when(sourceConfig.getEncryptionConfig()).thenReturn(null);
        when(sourceConfig.getBootStrapServers()).thenReturn(null);
        when(kafkaClusterConfigSupplier.getBootStrapServers()).thenReturn(bootstrapServers);
        when(kafkaClusterConfigSupplier.getAuthConfig()).thenReturn(authConfig);
        when(kafkaClusterConfigSupplier.getAwsConfig()).thenReturn(awsConfig);
        when(kafkaClusterConfigSupplier.getEncryptionConfig()).thenReturn(encryptionConfig);
        try (MockedStatic<KafkaSourceSecurityConfigurer> mockedStatic = mockStatic(KafkaSourceSecurityConfigurer.class)) {
            mockedStatic.when(() -> KafkaSourceSecurityConfigurer.setAuthProperties(any(), any(), any())).thenAnswer((Answer<Void>) invocation -> null);
            kafkaConnectSource = createSourceUnderTest();
            verify(sourceConfig).setBootstrapServers(String.join(",", bootstrapServers));
            verify(sourceConfig).setAuthConfig(authConfig);
            verify(sourceConfig).setAwsConfig(awsConfig);
            verify(sourceConfig).setEncryptionConfig(encryptionConfig);
        }
    }

    @Test
    void test_updateConfig_not_using_kafkaClusterConfigExtension() {
        final List<String> bootstrapServers = List.of("localhost:9092");
        final AuthConfig authConfig = mock(AuthConfig.class);
        final AwsConfig awsConfig = mock(AwsConfig.class);
        final EncryptionConfig encryptionConfig = mock(EncryptionConfig.class);
        lenient().doNothing().when(sourceConfig).setBootstrapServers(any());
        lenient().doNothing().when(sourceConfig).setAuthConfig(any());
        lenient().doNothing().when(sourceConfig).setAwsConfig(any());
        lenient().doNothing().when(sourceConfig).setEncryptionConfig(any());
        lenient().when(sourceConfig.getAuthConfig()).thenReturn(authConfig);
        lenient().when(sourceConfig.getAwsConfig()).thenReturn(awsConfig);
        lenient().when(sourceConfig.getEncryptionConfig()).thenReturn(encryptionConfig);
        lenient().when(sourceConfig.getBootStrapServers()).thenReturn(String.join(",", bootstrapServers));
        try (MockedStatic<KafkaSourceSecurityConfigurer> mockedStatic = mockStatic(KafkaSourceSecurityConfigurer.class)) {
            mockedStatic.when(() -> KafkaSourceSecurityConfigurer.setAuthProperties(any(), any(), any())).thenAnswer((Answer<Void>) invocation -> null);
            kafkaConnectSource = createSourceUnderTest();
            verify(sourceConfig, never()).setBootstrapServers(any());
            verify(sourceConfig, never()).setAuthConfig(any());
            verify(sourceConfig, never()).setAwsConfig(any());
            verify(sourceConfig, never()).setEncryptionConfig(any());
        }
    }
}
