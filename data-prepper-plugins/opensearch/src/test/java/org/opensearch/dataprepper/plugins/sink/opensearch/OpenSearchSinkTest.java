/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig;
import org.opensearch.dataprepper.plugins.sink.opensearch.dlq.FailedDlqData;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.DocumentBuilder;
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
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.model.sink.SinkLatencyMetrics.EXTERNAL_LATENCY;
import static org.opensearch.dataprepper.model.sink.SinkLatencyMetrics.INTERNAL_LATENCY;
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
    private Timer bulkRequestTimer;

    @Mock
    private OpenSearchSinkConfig openSearchSinkConfig;

    @Mock
    private Counter bulkRequestErrorsCounter;

    @Mock
    private Counter invalidActionErrorsCounter;

    @Mock
    private Counter dynamicIndexDroppedEvents;

    @Mock
    private DistributionSummary bulkRequestSizeBytesSummary;

    @Mock
    private Counter dynamicDocumentVersionDroppedEvents;

    @Mock
    private PluginConfigObservable pluginConfigObservable;

    @BeforeEach
    void setup() {
        when(pipelineDescription.getPipelineName()).thenReturn(UUID.randomUUID().toString());

        final RetryConfiguration retryConfiguration = mock(RetryConfiguration.class);
        when(retryConfiguration.getDlq()).thenReturn(Optional.empty());
        lenient().when(retryConfiguration.getDlqFile()).thenReturn(null);

        final ConnectionConfiguration connectionConfiguration = mock(ConnectionConfiguration.class);
        final RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        lenient().when(connectionConfiguration.createClient(awsCredentialsSupplier)).thenReturn(restHighLevelClient);
        lenient().when(connectionConfiguration.createOpenSearchClient(restHighLevelClient, awsCredentialsSupplier)).thenReturn(openSearchClient);

        when(indexConfiguration.getAction()).thenReturn("index");
        when(indexConfiguration.getDocumentId()).thenReturn(null);
        when(indexConfiguration.getDocumentIdField()).thenReturn(null);
        when(indexConfiguration.getRouting()).thenReturn(null);
        when(indexConfiguration.getActions()).thenReturn(null);
        when(indexConfiguration.getDocumentRootKey()).thenReturn(null);
        lenient().when(indexConfiguration.getVersionType()).thenReturn(null);
        lenient().when(indexConfiguration.getVersionExpression()).thenReturn(null);
        lenient().when(indexConfiguration.getIndexAlias()).thenReturn(UUID.randomUUID().toString());
        lenient().when(indexConfiguration.getTemplateType()).thenReturn(TemplateType.V1);
        when(indexConfiguration.getIndexType()).thenReturn(IndexType.CUSTOM);
        when(indexConfiguration.getBulkSize()).thenReturn(DEFAULT_BULK_SIZE);
        when(indexConfiguration.getFlushTimeout()).thenReturn(DEFAULT_FLUSH_TIMEOUT);

        when(openSearchSinkConfiguration.getIndexConfiguration()).thenReturn(indexConfiguration);
        when(openSearchSinkConfiguration.getRetryConfiguration()).thenReturn(retryConfiguration);
        lenient().when(openSearchSinkConfiguration.getConnectionConfiguration()).thenReturn(connectionConfiguration);

        when(pluginMetrics.counter(MetricNames.RECORDS_IN)).thenReturn(mock(Counter.class));
        when(pluginMetrics.timer(MetricNames.TIME_ELAPSED)).thenReturn(mock(Timer.class));
        when(pluginMetrics.timer(INTERNAL_LATENCY)).thenReturn(mock(Timer.class));
        when(pluginMetrics.timer(EXTERNAL_LATENCY)).thenReturn(mock(Timer.class));
        when(pluginMetrics.timer(BULKREQUEST_LATENCY)).thenReturn(bulkRequestTimer);
        when(pluginMetrics.counter(BULKREQUEST_ERRORS)).thenReturn(bulkRequestErrorsCounter);
        when(pluginMetrics.counter(INVALID_ACTION_ERRORS)).thenReturn(invalidActionErrorsCounter);
        when(pluginMetrics.counter(DYNAMIC_INDEX_DROPPED_EVENTS)).thenReturn(dynamicIndexDroppedEvents);
        when(pluginMetrics.counter(INVALID_VERSION_EXPRESSION_DROPPED_EVENTS)).thenReturn(dynamicDocumentVersionDroppedEvents);
        when(pluginMetrics.summary(BULKREQUEST_SIZE_BYTES)).thenReturn(bulkRequestSizeBytesSummary);

        lenient().when(sinkContext.getTagsTargetKey()).thenReturn(null);
        lenient().when(sinkContext.getIncludeKeys()).thenReturn(null);
        lenient().when(sinkContext.getExcludeKeys()).thenReturn(null);
    }

    private OpenSearchSink createObjectUnderTest() throws IOException {
        try (final MockedStatic<OpenSearchSinkConfiguration> openSearchSinkConfigurationMockedStatic = mockStatic(OpenSearchSinkConfiguration.class);
             final MockedStatic<PluginMetrics> pluginMetricsMockedStatic = mockStatic(PluginMetrics.class);
             final MockedConstruction<IndexManagerFactory> indexManagerFactoryMockedConstruction = mockConstruction(IndexManagerFactory.class, (mock, context) -> {
                 indexManagerFactory = mock;
             })) {
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
    }

    @Test
    void doOutput_with_invalid_version_expression_catches_NumberFormatException_and_creates_DLQObject() throws IOException {
        when(pluginSetting.getName()).thenReturn("opensearch");
        final String versionExpression = UUID.randomUUID().toString();
        when(indexConfiguration.getVersionExpression()).thenReturn(versionExpression);

        final Event event = mock(JacksonEvent.class);
        final String document = UUID.randomUUID().toString();
        when(event.toJsonString()).thenReturn(document);
        final EventHandle eventHandle = mock(EventHandle.class);
        when(event.getEventHandle()).thenReturn(eventHandle);
        final String index = UUID.randomUUID().toString();
        when(event.formatString(versionExpression, expressionEvaluator)).thenReturn("not_a_number");
        when(event.formatString(indexConfiguration.getIndexAlias(), expressionEvaluator)).thenReturn(index);
        final Record<Event> eventRecord = new Record<>(event);

        final OpenSearchSink objectUnderTest = createObjectUnderTest();
        when(indexManagerFactory.getIndexManager(any(IndexType.class), eq(openSearchClient), any(RestHighLevelClient.class), eq(openSearchSinkConfiguration), any(TemplateStrategy.class), any()))
                .thenReturn(indexManager);
        doNothing().when(indexManager).setupIndex();
        objectUnderTest.initialize();

        when(indexManager.getIndexName(anyString())).thenReturn(index);

        final DlqObject dlqObject = mock(DlqObject.class);

        final DlqObject.Builder dlqObjectBuilder = mock(DlqObject.Builder.class);
        final ArgumentCaptor<FailedDlqData> failedDlqData = ArgumentCaptor.forClass(FailedDlqData.class);
        when(dlqObjectBuilder.withEventHandle(eventHandle)).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withFailedData(failedDlqData.capture())).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPluginName(pluginSetting.getName())).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPluginId(pluginSetting.getName())).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPipelineName(pipelineDescription.getPipelineName())).thenReturn(dlqObjectBuilder);

        when(dlqObject.getFailedData()).thenReturn(mock(FailedDlqData.class));
        doNothing().when(dlqObject).releaseEventHandle(false);
        when(dlqObjectBuilder.build()).thenReturn(dlqObject);

        try (final MockedStatic<DocumentBuilder> documentBuilderMockedStatic = mockStatic(DocumentBuilder.class);
             final MockedStatic<DlqObject> dlqObjectMockedStatic = mockStatic(DlqObject.class)) {
            documentBuilderMockedStatic.when(() -> DocumentBuilder.build(eq(event), eq(null), eq(null), eq(null), eq(null)))
                    .thenReturn(UUID.randomUUID().toString());

            dlqObjectMockedStatic.when(DlqObject::builder).thenReturn(dlqObjectBuilder);
            objectUnderTest.doOutput(List.of(eventRecord));
        }

        final FailedDlqData failedDlqDataResult = failedDlqData.getValue();
        assertThat(failedDlqDataResult, notNullValue());
        assertThat(failedDlqDataResult.getDocument(), equalTo(document));
        assertThat(failedDlqDataResult.getIndex(), equalTo(index));
        assertThat(failedDlqDataResult.getMessage().startsWith("Unable to convert the result of evaluating document_version"), equalTo(true));

        verify(dynamicDocumentVersionDroppedEvents).increment();
    }

    @Test
    void test_routing_field_in_document() throws IOException {
        String routingFieldKey = UUID.randomUUID().toString();
        String routingKey = UUID.randomUUID().toString();
        String routingFieldValue = UUID.randomUUID().toString();
        when(indexConfiguration.getRoutingField()).thenReturn(routingFieldKey);
        when(indexConfiguration.getRouting()).thenReturn(routingKey);
        final OpenSearchSink objectUnderTest = createObjectUnderTest();
        final Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(Collections.singletonMap(routingFieldKey, routingFieldValue))
                .build();
        assertThat(objectUnderTest.getDocument(event).getRoutingField(), equalTo(Optional.of(routingFieldValue)));
    }

    @Test
    void test_routing_in_document() throws IOException {
        String routingValue = UUID.randomUUID().toString();
        String routingKey = UUID.randomUUID().toString();
        final OpenSearchSink objectUnderTest = createObjectUnderTest();
        final Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(Collections.singletonMap(routingKey, routingValue))
                .build();
        assertThat(objectUnderTest.getDocument(event).getRoutingField(), equalTo(Optional.empty()));

        when(indexConfiguration.getRouting()).thenReturn("${"+routingKey+"}");
        final OpenSearchSink objectUnderTest2 = createObjectUnderTest();
        assertThat(objectUnderTest2.getDocument(event).getRoutingField(), equalTo(Optional.of(routingValue)));
    }

    @Test
    void doOutput_with_invalid_version_expression_result_catches_RuntimeException_and_creates_DLQObject() throws IOException {

        when(pluginSetting.getName()).thenReturn("opensearch");
        final String versionExpression = UUID.randomUUID().toString();
        when(indexConfiguration.getVersionExpression()).thenReturn(versionExpression);

        final Event event = mock(JacksonEvent.class);
        final String document = UUID.randomUUID().toString();
        when(event.toJsonString()).thenReturn(document);
        final EventHandle eventHandle = mock(EventHandle.class);
        when(event.getEventHandle()).thenReturn(eventHandle);
        final String index = UUID.randomUUID().toString();
        when(event.formatString(versionExpression, expressionEvaluator)).thenThrow(RuntimeException.class);
        when(event.formatString(indexConfiguration.getIndexAlias(), expressionEvaluator)).thenReturn(index);
        final Record<Event> eventRecord = new Record<>(event);

        final OpenSearchSink objectUnderTest = createObjectUnderTest();
        when(indexManagerFactory.getIndexManager(any(IndexType.class), eq(openSearchClient), any(RestHighLevelClient.class), eq(openSearchSinkConfiguration), any(TemplateStrategy.class), any()))
                .thenReturn(indexManager);
        doNothing().when(indexManager).setupIndex();
        objectUnderTest.initialize();

        when(indexManager.getIndexName(anyString())).thenReturn(index);

        final DlqObject dlqObject = mock(DlqObject.class);

        final DlqObject.Builder dlqObjectBuilder = mock(DlqObject.Builder.class);
        final ArgumentCaptor<FailedDlqData> failedDlqData = ArgumentCaptor.forClass(FailedDlqData.class);
        when(dlqObjectBuilder.withEventHandle(eventHandle)).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withFailedData(failedDlqData.capture())).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPluginName(pluginSetting.getName())).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPluginId(pluginSetting.getName())).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPipelineName(pipelineDescription.getPipelineName())).thenReturn(dlqObjectBuilder);

        when(dlqObject.getFailedData()).thenReturn(mock(FailedDlqData.class));
        doNothing().when(dlqObject).releaseEventHandle(false);
        when(dlqObjectBuilder.build()).thenReturn(dlqObject);

        try (final MockedStatic<DocumentBuilder> documentBuilderMockedStatic = mockStatic(DocumentBuilder.class);
             final MockedStatic<DlqObject> dlqObjectMockedStatic = mockStatic(DlqObject.class)) {
            documentBuilderMockedStatic.when(() -> DocumentBuilder.build(eq(event), eq(null), eq(null), eq(null), eq(null)))
                    .thenReturn(UUID.randomUUID().toString());

            dlqObjectMockedStatic.when(DlqObject::builder).thenReturn(dlqObjectBuilder);
            objectUnderTest.doOutput(List.of(eventRecord));
        }

        final FailedDlqData failedDlqDataResult = failedDlqData.getValue();
        assertThat(failedDlqDataResult, notNullValue());
        assertThat(failedDlqDataResult.getDocument(), equalTo(document));
        assertThat(failedDlqDataResult.getIndex(), equalTo(index));
        assertThat(failedDlqDataResult.getMessage().startsWith("There was an exception when evaluating the document_version"), equalTo(true));

        verify(dynamicDocumentVersionDroppedEvents).increment();
    }
}
