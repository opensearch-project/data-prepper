package org.opensearch.dataprepper.plugins.sink.prometheus;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import static org.mockito.ArgumentMatchers.any;

import org.opensearch.dataprepper.plugins.sink.prometheus.service.PrometheusSinkService;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkThresholdConfig;
import org.opensearch.dataprepper.aws.api.AwsConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Optional;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrometheusSinkTest {

    PrometheusSink prometheusSink;

    private PluginSetting pluginSetting;
    private PrometheusSinkConfiguration prometheusSinkConfiguration;

    private PipelineDescription pipelineDescription;

    @Mock
    private AwsConfig awsConfig;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;
    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;
    @Mock
    private PrometheusSinkService prometheusSinkService;

    @Mock
    private Counter counter;

    @Mock
    private DistributionSummary summary;

    private SinkContext sinkContext;
    private PluginMetrics pluginMetrics;

    private AwsAuthenticationOptions awsAuthenticationOptions;

    @BeforeEach
    void setUp() {
        pluginSetting = mock(PluginSetting.class);
        pluginMetrics = mock(PluginMetrics.class);
        prometheusSinkService = mock(PrometheusSinkService.class);
        prometheusSinkConfiguration = mock(PrometheusSinkConfiguration.class);
        pipelineDescription = mock(PipelineDescription.class);
        sinkContext = mock(SinkContext.class);
        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(pluginSetting.getPipelineName()).thenReturn("log-pipeline");
        PluginModel codecConfiguration = new PluginModel("http", new HashMap<>());
        awsCredentialsProvider = mock(AwsCredentialsProvider.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);
        when(awsCredentialsSupplier.getDefaultRegion()).thenReturn(Optional.of(Region.of("us-west-2")));
        counter = mock(Counter.class);
        summary = mock(DistributionSummary.class);
        when(pluginMetrics.summary(any(String.class))).thenReturn(summary);
        when(pluginMetrics.counter(any())).thenReturn(counter);
        awsConfig = mock(AwsConfig.class);
        when(awsConfig.getAwsRegion()).thenReturn(Region.of("us-west-2"));
        when(prometheusSinkConfiguration.getAwsConfig()).thenReturn(awsConfig);
        PrometheusSinkThresholdConfig thresholdConfig = mock(PrometheusSinkThresholdConfig.class);
        when(thresholdConfig.getFlushInterval()).thenReturn(10000L);
        when(thresholdConfig.getMaxEvents()).thenReturn(100);
        when(thresholdConfig.getMaxRequestSizeBytes()).thenReturn(10000L);

        when(prometheusSinkConfiguration.getThresholdConfig()).thenReturn(thresholdConfig);
        when(prometheusSinkConfiguration.getEncoding()).thenReturn(prometheusSinkConfiguration.DEFAULT_ENCODING);
        when(prometheusSinkConfiguration.getConnectionTimeout()).thenReturn(Duration.ofSeconds(2));
        when(prometheusSinkConfiguration.getUrl()).thenReturn("http://localhost:8080");
        Map<String, Object> dlqSetting = new HashMap<>();
        dlqSetting.put("bucket", "dlq.test");
        dlqSetting.put("key_path_prefix", "\\dlq");
        PluginModel dlq = new PluginModel("s3",dlqSetting);
    }

    private PrometheusSink createObjectUnderTest() {
        return new PrometheusSink(pluginSetting, pluginMetrics, pipelineDescription,  prometheusSinkConfiguration, awsCredentialsSupplier);
    }
    @Test
    void test_http_sink_plugin_isReady_positive() {
        prometheusSink = createObjectUnderTest();
        Assertions.assertNotNull(prometheusSink);
        prometheusSink.doInitialize();
        assertTrue(prometheusSink.isReady(), "http sink is initialized and ready to work");
    }

    @Test
    void test_http_Sink_plugin_isReady_negative() {
        prometheusSink = createObjectUnderTest();
        Assertions.assertNotNull(prometheusSink);
        assertFalse(prometheusSink.isReady(), "httpSink sink is not initialized and not ready to work");
    }

    @Test
    void test_doOutput_with_empty_records() {
        prometheusSink = createObjectUnderTest();
        Assertions.assertNotNull(prometheusSink);
        prometheusSink.doInitialize();
        Collection<Record<Event>> records = new ArrayList<>();
        prometheusSink.doOutput(records);
    }
}
