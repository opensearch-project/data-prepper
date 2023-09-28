/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3ObjectPluginMetricsTest {
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private DistributionSummary summary;
    @Mock
    private Timer s3ObjectReadTimer;
    @Mock
    private Counter counter;
    @Test
    public void s3ObjectPluginMetricsTest(){
        pluginMetrics = mock(PluginMetrics.class);
        when(pluginMetrics.counter(S3ObjectPluginMetrics.S3_OBJECTS_FAILED_METRIC_NAME)).thenReturn(counter);
        when(pluginMetrics.counter(S3ObjectPluginMetrics.S3_OBJECTS_FAILED_NOT_FOUND_METRIC_NAME)).thenReturn(counter);
        when(pluginMetrics.counter(S3ObjectPluginMetrics.S3_OBJECTS_FAILED_NOT_FOUND_ACCESS_DENIED)).thenReturn(counter);
        when(pluginMetrics.counter(S3ObjectPluginMetrics.S3_OBJECTS_SUCCEEDED_METRIC_NAME)).thenReturn(counter);
        when(pluginMetrics.timer(S3ObjectPluginMetrics.S3_OBJECTS_TIME_ELAPSED_METRIC_NAME)).thenReturn(s3ObjectReadTimer);
        when(pluginMetrics.summary(S3ObjectPluginMetrics.S3_OBJECTS_SIZE)).thenReturn(summary);
        when(pluginMetrics.summary(S3ObjectPluginMetrics.S3_OBJECTS_SIZE_PROCESSED)).thenReturn(summary);
        when(pluginMetrics.summary(S3ObjectPluginMetrics.S3_OBJECTS_EVENTS)).thenReturn(summary);
        S3ObjectPluginMetrics metrics = new S3ObjectPluginMetrics(pluginMetrics);
        assertThat(metrics.getS3ObjectEventsSummary(),sameInstance(summary));
        assertThat(metrics.getS3ObjectSizeSummary(),sameInstance(summary));
        assertThat(metrics.getS3ObjectSizeProcessedSummary(),sameInstance(summary));
        assertThat(metrics.getS3ObjectReadTimer(),sameInstance(s3ObjectReadTimer));
        assertThat(metrics.getS3ObjectsFailedCounter(),sameInstance(counter));
        assertThat(metrics.getS3ObjectsSucceededCounter(),sameInstance(counter));
        assertThat(metrics.getS3ObjectsFailedAccessDeniedCounter(),sameInstance(counter));
        assertThat(metrics.getS3ObjectsFailedNotFoundCounter(),sameInstance(counter));
    }
}
