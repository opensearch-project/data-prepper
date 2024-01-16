package org.opensearch.dataprepper.plugins.source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.regions.Region;

import java.time.Duration;
import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CloudwatchMetricsSourceTest {

    private final String PLUGIN_NAME = "cloudwatch";
    private final String TEST_PIPELINE_NAME = "cloudwatch-test-pipeline";

    private CloudwatchMetricsSource cloudwatchMetricsSource;
    private PluginMetrics pluginMetrics;
    private CloudwatchMetricsSourceConfig cloudwatchMetricsSourceConfig;
    private PluginFactory pluginFactory;
    private AcknowledgementSetManager acknowledgementSetManager;
    private AwsCredentialsSupplier awsCredentialsSupplier;

    private int recordsToAccumulate = 100;

    private Duration bufferTimeout = Duration.ofSeconds(10);

    private AwsAuthenticationOptions awsAuthenticationOptions;


    @BeforeEach
    void setUp() {
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);
        cloudwatchMetricsSourceConfig = mock(CloudwatchMetricsSourceConfig.class);
        pluginFactory = mock(PluginFactory.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.AF_SOUTH_1);
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn("test-arn");
        when(cloudwatchMetricsSourceConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        cloudwatchMetricsSource = new CloudwatchMetricsSource(pluginMetrics, cloudwatchMetricsSourceConfig,awsCredentialsSupplier);
    }

    @Test
    void start_should_throw_IllegalStateException_when_buffer_is_null() {
        assertThrows(IllegalStateException.class, () -> cloudwatchMetricsSource.start(null));
    }

    private BlockingBuffer<Record<Event>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 2);
        integerHashMap.put("batch_size", 2);
        final PluginSetting pluginSetting = new PluginSetting("blocking_buffer", integerHashMap);
        pluginSetting.setPipelineName("pipeline");
        return new BlockingBuffer<>(pluginSetting);
    }

    @Test
    void start_with_empty_buffer_test() {
        final BlockingBuffer<Record<Event>> buffer = getBuffer();
        assertThat(buffer.isEmpty(),equalTo(true));
    }
}
