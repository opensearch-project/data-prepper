package org.opensearch.dataprepper.plugins.source;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.source.configuration.DimensionConfig;
import org.opensearch.dataprepper.plugins.source.configuration.DimensionsListConfig;
import org.opensearch.dataprepper.plugins.source.configuration.MetricsConfig;
import org.opensearch.dataprepper.plugins.source.configuration.NamespaceConfig;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricDataResponse;
import software.amazon.awssdk.services.cloudwatch.model.MetricDataResult;
import software.amazon.awssdk.services.cloudwatch.model.MetricStat;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CloudwatchMetricsWorkerTest {

    private CloudWatchClient cloudWatchClient;

    private CloudwatchMetricsSourceConfig cloudwatchMetricsSourceConfig;

    private NamespaceConfig namespaceConfig;

    private PluginMetrics pluginMetrics;

    private GetMetricDataResponse getMetricDataResponse;

    private MetricsConfig metricsConfig;

    private DimensionsListConfig dimensionsListConfig;

    private SourceCoordinator sourceCoordinator;

    MetricDataResult metricDataResultMock = MetricDataResult.builder()
            .id("BucketSizeBytes")
            .statusCode("SUCCESS")
            .values(1.10)
            .build();

    @BeforeEach
    public void setup(){
        final String namespaceName = "AWS/S3";
        this.sourceCoordinator = mock(SourceCoordinator.class);
        this.cloudWatchClient = mock(CloudWatchClient.class);
        this.namespaceConfig = mock(NamespaceConfig.class);
        when(namespaceConfig.getName()).thenReturn(namespaceName);
        this.cloudwatchMetricsSourceConfig = mock(CloudwatchMetricsSourceConfig.class);

        this.metricsConfig = mock(MetricsConfig.class);
        DimensionConfig dimensionConfig = mock(DimensionConfig.class);
        when(dimensionConfig.getName()).thenReturn("StorageType");
        when(dimensionConfig.getValue()).thenReturn("StandardStorage");
        this.dimensionsListConfig = mock(DimensionsListConfig.class);
        when(dimensionsListConfig.getDimensionConfig()).thenReturn(dimensionConfig);
        when(metricsConfig.getName()).thenReturn("test-metric");
        when(metricsConfig.getDimensionsListConfigs()).thenReturn(List.of(dimensionsListConfig));

        when(namespaceConfig.getStartTime()).thenReturn("2023-05-19T18:35:24z");
        when(namespaceConfig.getEndTime()).thenReturn("2023-08-07T18:35:24z");
        when(namespaceConfig.getMetricsConfigs()).thenReturn(List.of(metricsConfig));
        when(cloudwatchMetricsSourceConfig.getNamespaceConfigs()).thenReturn(List.of(namespaceConfig));
        this.getMetricDataResponse = mock(GetMetricDataResponse.class);
        when(getMetricDataResponse.metricDataResults()).thenReturn(List.of(metricDataResultMock));
        when(cloudWatchClient.getMetricData(any(GetMetricDataRequest.class))).thenReturn(getMetricDataResponse);
    }

    private BlockingBuffer<Record<Event>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 2);
        integerHashMap.put("batch_size", 2);
        final PluginSetting pluginSetting = new PluginSetting("blocking_buffer", integerHashMap);
        pluginSetting.setPipelineName("pipeline");
        return new BlockingBuffer<>(pluginSetting);
    }

    private CloudwatchMetricsWorker createObjectUnderTest(final Buffer<Record<Event>> buffer){
        return new CloudwatchMetricsWorker(cloudWatchClient,cloudwatchMetricsSourceConfig,pluginMetrics, sourceCoordinator,buffer);
    }

    @Test
    public void cloud_watch_metrics_test() throws Exception {
        final String partitionKey = "AWS/S3" + "@" + "test-metric";
        final SourcePartition<CloudwatchSourceProgressState> partitionToProcess = SourcePartition.builder(CloudwatchSourceProgressState.class)
                .withPartitionKey(partitionKey)
                .withPartitionClosedCount(0L)
                .build();

        given(sourceCoordinator.getNextPartition(any(Function.class))).willReturn(Optional.of(partitionToProcess));
        final BlockingBuffer<Record<Event>> buffer = getBuffer();
        final ArgumentCaptor<GetMetricDataRequest> getMetricDataRequestCaptor = ArgumentCaptor.forClass(GetMetricDataRequest.class);
        createObjectUnderTest(buffer).run();
        verify(cloudWatchClient).getMetricData(getMetricDataRequestCaptor.capture());
        final GetMetricDataRequest getMetricDataRequestCaptorValue = getMetricDataRequestCaptor.getValue();
        assertThat(getMetricDataRequestCaptorValue.endTime().toString(),equalTo("2023-08-07T18:35:24Z"));
        assertThat(getMetricDataRequestCaptorValue.startTime().toString(),equalTo("2023-05-19T18:35:24Z"));
        final MetricStat metricStat = getMetricDataRequestCaptorValue.metricDataQueries().get(0).metricStat();
        assertThat(metricStat.metric().namespace(),equalTo("AWS/S3"));
        assertThat(metricStat.metric().metricName(),equalTo("test-metric"));
        assertThat(metricStat.metric().dimensions().get(0).name(),equalTo("StorageType"));
        assertThat(metricStat.metric().dimensions().get(0).value(),equalTo("StandardStorage"));
    }



}
