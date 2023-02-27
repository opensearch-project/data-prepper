/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.metricpublisher;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.metrics.MetricRecord;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.metrics.internal.DefaultMetricCollection;
import software.amazon.awssdk.metrics.internal.DefaultMetricRecord;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class MicrometerMetricPublisherTest {
    private static final String SERVICE_ID = "ServiceId";
    private static final String OPERATION_NAME = "OperationName";
    private static final String SERVICE_ID_VALUE = "ServiceIdValue";
    private static final String OPERATION_NAME_VALUE = "OperationNameValue";

    private static SdkMetric<String> serviceIdMetric;
    private static SdkMetric<String> operationNameMetric;
    private static DefaultMetricRecord<String> serviceIdMetricRecord;
    private static DefaultMetricRecord<String> operationNameMetricRecord;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private SdkMetric sdkMetric;

    @Mock
    private Counter counter;

    @Mock
    private Timer timer;

    @BeforeAll
    static void setup() {
        serviceIdMetric = CoreMetric.SERVICE_ID;
        operationNameMetric = CoreMetric.OPERATION_NAME;

        serviceIdMetricRecord = new DefaultMetricRecord<>(serviceIdMetric, SERVICE_ID_VALUE);
        operationNameMetricRecord = new DefaultMetricRecord<>(operationNameMetric, OPERATION_NAME_VALUE);
    }

    private MicrometerMetricPublisher createObjectUnderTest() {
        return new MicrometerMetricPublisher(pluginMetrics);
    }

    @ParameterizedTest
    @ValueSource(strings = {"PT0.44334814S", "PT0.000005051S"})
    void testDurationMetricRecord(final String metricValue) {
        final String metricName = UUID.randomUUID().toString();
        when(pluginMetrics.timerWithTags(metricName, SERVICE_ID, SERVICE_ID_VALUE, OPERATION_NAME, OPERATION_NAME_VALUE)).thenReturn(timer);

        when(sdkMetric.name()).thenReturn(metricName);
        when(sdkMetric.valueClass()).thenReturn(Duration.class);

        final MetricRecord metricRecordMock = mock(MetricRecord.class);
        when(metricRecordMock.metric()).thenReturn(sdkMetric);
        when(metricRecordMock.value()).thenReturn(metricValue);

        final Map<SdkMetric<?>, List<MetricRecord<?>>> metrics = new HashMap<>();
        metrics.put(sdkMetric, List.of(metricRecordMock, serviceIdMetricRecord, operationNameMetricRecord));

        final DefaultMetricCollection metricCollection = new DefaultMetricCollection("Collection Name", metrics, null);

        final MicrometerMetricPublisher objectUnderTest = createObjectUnderTest();
        objectUnderTest.publish(metricCollection);

        verify(sdkMetric).name();
        verify(sdkMetric).valueClass();
        verify(metricRecordMock, times(3)).metric();
        verify(metricRecordMock).value();

        verify(timer).record(Duration.parse(metricValue).toMillis(), TimeUnit.MILLISECONDS);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 5})
    void testIntegerMetricRecord(final int metricValue) {
        final String metricName = UUID.randomUUID().toString();
        when(pluginMetrics.counterWithTags(metricName, SERVICE_ID, SERVICE_ID_VALUE, OPERATION_NAME, OPERATION_NAME_VALUE)).thenReturn(counter);

        when(sdkMetric.name()).thenReturn(metricName);
        when(sdkMetric.valueClass()).thenReturn(Integer.class);


        final MetricRecord metricRecordMock = mock(MetricRecord.class);
        when(metricRecordMock.metric()).thenReturn(sdkMetric);
        when(metricRecordMock.value()).thenReturn(metricValue);

        final Map<SdkMetric<?>, List<MetricRecord<?>>> metrics = new HashMap<>();
        metrics.put(sdkMetric, List.of(metricRecordMock, serviceIdMetricRecord, operationNameMetricRecord));

        final DefaultMetricCollection metricCollection = new DefaultMetricCollection("Collection Name", metrics, null);

        final MicrometerMetricPublisher objectUnderTest = createObjectUnderTest();
        objectUnderTest.publish(metricCollection);

        verify(sdkMetric).name();
        verify(sdkMetric).valueClass();
        verify(metricRecordMock, times(3)).metric();
        verify(metricRecordMock).value();

        verify(counter).increment(metricValue);
    }

    @ParameterizedTest
    @ValueSource(doubles = {1.0, 3.0})
    void testDoubleMetricRecord(final double metricValue) {
        final String metricName = UUID.randomUUID().toString();
        when(pluginMetrics.counterWithTags(metricName, SERVICE_ID, SERVICE_ID_VALUE, OPERATION_NAME, OPERATION_NAME_VALUE)).thenReturn(counter);

        when(sdkMetric.name()).thenReturn(metricName);
        when(sdkMetric.valueClass()).thenReturn(Integer.class);


        final MetricRecord metricRecordMock = mock(MetricRecord.class);
        when(metricRecordMock.metric()).thenReturn(sdkMetric);
        when(metricRecordMock.value()).thenReturn(metricValue);

        final Map<SdkMetric<?>, List<MetricRecord<?>>> metrics = new HashMap<>();
        metrics.put(sdkMetric, List.of(metricRecordMock, serviceIdMetricRecord, operationNameMetricRecord));

        final DefaultMetricCollection metricCollection = new DefaultMetricCollection("Collection Name", metrics, null);

        final MicrometerMetricPublisher objectUnderTest = createObjectUnderTest();
        objectUnderTest.publish(metricCollection);

        verify(sdkMetric).name();
        verify(sdkMetric).valueClass();
        verify(metricRecordMock, times(3)).metric();
        verify(metricRecordMock).value();

        verify(counter).increment(metricValue);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testBooleanMetricRecord(final boolean metricValue) {
        final String metricName = UUID.randomUUID().toString();
        when(pluginMetrics.counterWithTags(metricName, SERVICE_ID, SERVICE_ID_VALUE, OPERATION_NAME, OPERATION_NAME_VALUE)).thenReturn(counter);

        when(sdkMetric.name()).thenReturn(metricName);
        when(sdkMetric.valueClass()).thenReturn(Boolean.class);

        final MetricRecord metricRecordMock = mock(MetricRecord.class);
        when(metricRecordMock.metric()).thenReturn(sdkMetric);
        when(metricRecordMock.value()).thenReturn(metricValue);

        final Map<SdkMetric<?>, List<MetricRecord<?>>> metrics = new HashMap<>();
        metrics.put(sdkMetric, List.of(metricRecordMock, serviceIdMetricRecord, operationNameMetricRecord));

        final DefaultMetricCollection metricCollection = new DefaultMetricCollection("Collection Name", metrics, null);

        final MicrometerMetricPublisher objectUnderTest = createObjectUnderTest();
        objectUnderTest.publish(metricCollection);

        verify(sdkMetric).name();
        verify(sdkMetric).valueClass();
        verify(metricRecordMock, times(3)).metric();
        verify(metricRecordMock).value();

        final double incrementCount = metricValue ? 1 : 0;
        verify(counter).increment(incrementCount);
    }

    @Test
    void testIntegerMetricRecordWithChildren() {
        final int metricValue = 10;
        final String metricName = UUID.randomUUID().toString();
        when(pluginMetrics.counterWithTags(metricName, SERVICE_ID, SERVICE_ID_VALUE, OPERATION_NAME, OPERATION_NAME_VALUE)).thenReturn(counter);

        when(sdkMetric.name()).thenReturn(metricName);
        when(sdkMetric.valueClass()).thenReturn(Integer.class);


        final MetricRecord metricRecordMock = mock(MetricRecord.class);
        when(metricRecordMock.metric()).thenReturn(sdkMetric);
        when(metricRecordMock.value()).thenReturn(metricValue);

        final Map<SdkMetric<?>, List<MetricRecord<?>>> metrics = new HashMap<>();
        metrics.put(sdkMetric, List.of(metricRecordMock, serviceIdMetricRecord, operationNameMetricRecord));

        final DefaultMetricCollection childMetricCollection = new DefaultMetricCollection("Child Collection Name", metrics, null);
        final DefaultMetricCollection metricCollection = new DefaultMetricCollection("Collection Name", metrics, List.of(childMetricCollection));

        final MicrometerMetricPublisher objectUnderTest = createObjectUnderTest();
        objectUnderTest.publish(metricCollection);

        verify(sdkMetric, times(2)).name();
        verify(sdkMetric, times(2)).valueClass();
        verify(metricRecordMock, times(5)).metric();
        verify(metricRecordMock, times(2)).value();

        verify(counter, times(2)).increment(metricValue);
    }
}
