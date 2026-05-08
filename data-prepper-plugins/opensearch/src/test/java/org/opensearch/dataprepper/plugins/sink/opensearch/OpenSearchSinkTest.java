/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexManager;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexManagerFactory;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexType;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.TemplateStrategy;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.TemplateType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.model.sink.SinkLatencyMetrics.EXTERNAL_LATENCY;
import static org.opensearch.dataprepper.model.sink.SinkLatencyMetrics.INTERNAL_LATENCY;
import static org.opensearch.dataprepper.metrics.MetricNames.RECORDS_IN;
import static org.opensearch.dataprepper.metrics.MetricNames.TIME_ELAPSED;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.BULKREQUEST_ERRORS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.BULKREQUEST_LATENCY;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.BULKREQUEST_SIZE_BYTES;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.DYNAMIC_INDEX_DROPPED_EVENTS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.INVALID_ACTION_ERRORS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.INVALID_VERSION_EXPRESSION_DROPPED_EVENTS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig.DEFAULT_BULK_SIZE;
import static org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig.DEFAULT_FLUSH_TIMEOUT;

@ExtendWith(MockitoExtension.class)
public class OpenSearchSinkTest {

    @Mock
    private IndexManagerFactory indexManagerFactory;

    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    private SinkContext sinkContext;

    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private OpenSearchSinkConfiguration openSearchSinkConfiguration;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    private IndexConfiguration indexConfiguration;

    @Mock
    private IndexManager indexManager;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private OpenSearchSinkConfig openSearchSinkConfig;

    @Mock
    private PluginConfigObservable pluginConfigObservable;

    @BeforeEach
    void setup() {
        when(pipelineDescription.getPipelineName()).thenReturn(UUID.randomUUID().toString());

        final RetryConfiguration retryConfiguration = mock(RetryConfiguration.class);
        lenient().when(retryConfiguration.getDlq()).thenReturn(Optional.empty());
        lenient().when(retryConfiguration.getDlqFile()).thenReturn(null);

        final ConnectionConfiguration connectionConfiguration = mock(ConnectionConfiguration.class);
        final RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        lenient().when(connectionConfiguration.createClient(awsCredentialsSupplier)).thenReturn(restHighLevelClient);
        lenient().when(connectionConfiguration.createOpenSearchClient(restHighLevelClient, awsCredentialsSupplier)).thenReturn(openSearchClient);

        lenient().when(indexConfiguration.getAction()).thenReturn("index");
        lenient().when(indexConfiguration.getDocumentId()).thenReturn(null);
        lenient().when(indexConfiguration.getDocumentIdField()).thenReturn(null);
        lenient().when(indexConfiguration.getRouting()).thenReturn(null);
        lenient().when(indexConfiguration.getActions()).thenReturn(null);
        lenient().when(indexConfiguration.getDocumentRootKey()).thenReturn(null);
        lenient().when(indexConfiguration.getVersionType()).thenReturn(null);
        lenient().when(indexConfiguration.getVersionExpression()).thenReturn(null);
        lenient().when(indexConfiguration.getIndexAlias()).thenReturn(UUID.randomUUID().toString());
        lenient().when(indexConfiguration.getTemplateType()).thenReturn(TemplateType.V1);
        lenient().when(indexConfiguration.getIndexType()).thenReturn(IndexType.CUSTOM);
        lenient().when(indexConfiguration.getBulkSize()).thenReturn(DEFAULT_BULK_SIZE);
        lenient().when(indexConfiguration.getFlushTimeout()).thenReturn(DEFAULT_FLUSH_TIMEOUT);

        lenient().when(openSearchSinkConfiguration.getIndexConfiguration()).thenReturn(indexConfiguration);
        lenient().when(openSearchSinkConfiguration.getRetryConfiguration()).thenReturn(retryConfiguration);
        lenient().when(openSearchSinkConfiguration.getConnectionConfiguration()).thenReturn(connectionConfiguration);

        lenient().when(pluginMetrics.counter(RECORDS_IN)).thenReturn(mock(Counter.class));
        lenient().when(pluginMetrics.timer(TIME_ELAPSED)).thenReturn(mock(Timer.class));
        lenient().when(pluginMetrics.timer(INTERNAL_LATENCY)).thenReturn(mock(Timer.class));
        lenient().when(pluginMetrics.timer(EXTERNAL_LATENCY)).thenReturn(mock(Timer.class));
        lenient().when(pluginMetrics.timer(BULKREQUEST_LATENCY)).thenReturn(mock(Timer.class));
        lenient().when(pluginMetrics.counter(BULKREQUEST_ERRORS)).thenReturn(mock(Counter.class));
        lenient().when(pluginMetrics.counter(INVALID_ACTION_ERRORS)).thenReturn(mock(Counter.class));
        lenient().when(pluginMetrics.counter(DYNAMIC_INDEX_DROPPED_EVENTS)).thenReturn(mock(Counter.class));
        lenient().when(pluginMetrics.counter(INVALID_VERSION_EXPRESSION_DROPPED_EVENTS)).thenReturn(mock(Counter.class));
        lenient().when(pluginMetrics.summary(BULKREQUEST_SIZE_BYTES)).thenReturn(mock(DistributionSummary.class));
        lenient().when(pluginMetrics.counter(anyString())).thenReturn(mock(Counter.class));

        lenient().when(sinkContext.getTagsTargetKey()).thenReturn(null);
        lenient().when(sinkContext.getIncludeKeys()).thenReturn(null);
        lenient().when(sinkContext.getExcludeKeys()).thenReturn(null);
    }

    private OpenSearchSink createObjectUnderTest() throws IOException {
        try (final MockedStatic<OpenSearchSinkConfiguration> openSearchSinkConfigurationMockedStatic = mockStatic(OpenSearchSinkConfiguration.class);
             final MockedStatic<PluginMetrics> pluginMetricsMockedStatic = mockStatic(PluginMetrics.class);
             final MockedConstruction<IndexManagerFactory> indexManagerFactoryMockedConstruction = mockConstruction(IndexManagerFactory.class, (mock, context) -> {
                 indexManagerFactory = mock;
             });
             final MockedConstruction<BulkIngester> bulkIngesterMockedConstruction = mockConstruction(BulkIngester.class)) {
            pluginMetricsMockedStatic.when(() -> PluginMetrics.fromPluginSetting(pluginSetting)).thenReturn(pluginMetrics);
            openSearchSinkConfigurationMockedStatic.when(() -> OpenSearchSinkConfiguration.readOSConfig(openSearchSinkConfig, expressionEvaluator))
                    .thenReturn(openSearchSinkConfiguration);
            return new OpenSearchSink(
                    pluginSetting, sinkContext, expressionEvaluator, awsCredentialsSupplier, pipelineDescription, pluginConfigObservable, openSearchSinkConfig);
        }
    }

    @Test
    void test_initialization() throws IOException {
        final OpenSearchSink objectUnderTest = createObjectUnderTest();
        when(indexManagerFactory.getIndexManager(any(IndexType.class), eq(openSearchClient), any(RestHighLevelClient.class), eq(openSearchSinkConfiguration), any(TemplateStrategy.class), any()))
                .thenReturn(indexManager);
        doNothing().when(indexManager).setupIndex();
        objectUnderTest.initialize();

        verify(pluginConfigObservable).addPluginConfigObserver(any());
        assertThat(objectUnderTest.isReady(), equalTo(true));
    }

    @Test
    void test_initialization_with_failure_and_retry() throws IOException {
        final OpenSearchSink objectUnderTest = createObjectUnderTest();
        when(indexManagerFactory.getIndexManager(any(IndexType.class), eq(openSearchClient), any(RestHighLevelClient.class), eq(openSearchSinkConfiguration), any(TemplateStrategy.class), any()))
                .thenThrow(RuntimeException.class).thenReturn(indexManager);
        doNothing().when(indexManager).setupIndex();
        objectUnderTest.initialize();
        objectUnderTest.initialize();
        verify(pluginConfigObservable, times(2)).addPluginConfigObserver(any());
    }

    @Test
    void doOutput_delegates_to_ingester() throws Exception {
        final OpenSearchSink objectUnderTest = createObjectUnderTest();

        final Ingester ingester = getField(objectUnderTest, "ingester");
        final List<Record<Event>> records = Collections.emptyList();
        objectUnderTest.doOutput(records);

        verify(ingester).output(records);
    }

    @Test
    void shutdown_delegates_to_ingester() throws Exception {
        final OpenSearchSink objectUnderTest = createObjectUnderTest();

        final Ingester ingester = getField(objectUnderTest, "ingester");
        objectUnderTest.shutdown();

        verify(ingester).shutdown();
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(final Object target, final String fieldName) throws Exception {
        java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
    }
}
