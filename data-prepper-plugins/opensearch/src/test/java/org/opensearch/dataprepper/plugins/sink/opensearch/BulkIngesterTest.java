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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.VersionType;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.sink.SinkForwardRecordsContext;
import org.opensearch.dataprepper.plugins.sink.opensearch.dlq.FailedDlqData;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.DocumentBuilder;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.BULKREQUEST_ERRORS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.BULKREQUEST_LATENCY;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.BULKREQUEST_SIZE_BYTES;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.DYNAMIC_INDEX_DROPPED_EVENTS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.INVALID_ACTION_ERRORS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.INVALID_VERSION_EXPRESSION_DROPPED_EVENTS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig.DEFAULT_BULK_SIZE;
import static org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig.DEFAULT_FLUSH_TIMEOUT;

@ExtendWith(MockitoExtension.class)
public class BulkIngesterTest {

    @Mock
    private SinkContext sinkContext;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private OpenSearchSinkConfiguration openSearchSinkConfiguration;

    @Mock
    private IndexConfiguration indexConfiguration;

    @Mock
    private IndexManager indexManager;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Timer bulkRequestTimer;

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
    private EventActionResolver eventActionResolver;

    private String pipeline;

    @BeforeEach
    void setup() {
        pipeline = UUID.randomUUID().toString();

        final RetryConfiguration retryConfiguration = mock(RetryConfiguration.class);
        when(retryConfiguration.getDlq()).thenReturn(Optional.empty());
        lenient().when(retryConfiguration.getDlqFile()).thenReturn(null);

        lenient().when(indexConfiguration.getAction()).thenReturn("index");
        lenient().when(indexConfiguration.getDocumentId()).thenReturn(null);
        lenient().when(indexConfiguration.getDocumentIdField()).thenReturn(null);
        lenient().when(indexConfiguration.getRouting()).thenReturn(null);
        lenient().when(indexConfiguration.getActions()).thenReturn(null);
        lenient().when(indexConfiguration.getDocumentRootKey()).thenReturn(null);
        lenient().when(indexConfiguration.getVersionType()).thenReturn(null);
        lenient().when(indexConfiguration.getVersionExpression()).thenReturn(null);
        lenient().when(indexConfiguration.getIndexAlias()).thenReturn(UUID.randomUUID().toString());
        lenient().when(indexConfiguration.getBulkSize()).thenReturn(DEFAULT_BULK_SIZE);
        lenient().when(indexConfiguration.getFlushTimeout()).thenReturn(DEFAULT_FLUSH_TIMEOUT);

        lenient().when(openSearchSinkConfiguration.getIndexConfiguration()).thenReturn(indexConfiguration);
        lenient().when(openSearchSinkConfiguration.getRetryConfiguration()).thenReturn(retryConfiguration);

        lenient().when(pluginMetrics.timer(BULKREQUEST_LATENCY)).thenReturn(bulkRequestTimer);
        lenient().when(pluginMetrics.counter(BULKREQUEST_ERRORS)).thenReturn(bulkRequestErrorsCounter);
        lenient().when(pluginMetrics.counter(INVALID_ACTION_ERRORS)).thenReturn(invalidActionErrorsCounter);
        lenient().when(pluginMetrics.counter(DYNAMIC_INDEX_DROPPED_EVENTS)).thenReturn(dynamicIndexDroppedEvents);
        lenient().when(pluginMetrics.counter(INVALID_VERSION_EXPRESSION_DROPPED_EVENTS)).thenReturn(dynamicDocumentVersionDroppedEvents);
        lenient().when(pluginMetrics.summary(BULKREQUEST_SIZE_BYTES)).thenReturn(bulkRequestSizeBytesSummary);

        lenient().when(sinkContext.getTagsTargetKey()).thenReturn(null);
        lenient().when(sinkContext.getIncludeKeys()).thenReturn(null);
        lenient().when(sinkContext.getExcludeKeys()).thenReturn(null);
    }

    private OpenSearchClient openSearchClient;
    private Supplier<OpenSearchClient> openSearchClientSupplier;

    private BulkIngester createObjectUnderTest() {
        openSearchClient = mock(OpenSearchClient.class);
        openSearchClientSupplier = () -> openSearchClient;
        return new BulkIngester(openSearchSinkConfiguration, expressionEvaluator, sinkContext,
                pluginMetrics, pipeline, eventActionResolver,
                openSearchClient, openSearchClientSupplier,
                () -> indexManager, () -> null, null);
    }

    @Test
    void test_successful_records_handling_without_forwarding_pipelines_bulk_operations_with_event_handles() {
        when(sinkContext.getForwardToPipelines()).thenReturn(Map.of());
        final BulkIngester objectUnderTest = createObjectUnderTest();
        BulkOperationWrapper op1 = mock(BulkOperationWrapper.class);
        BulkOperationWrapper op2 = mock(BulkOperationWrapper.class);
        when(op1.getEvent()).thenReturn(null);
        when(op2.getEvent()).thenReturn(null);
        List<BulkOperationWrapper> operationsList = new ArrayList<>();
        operationsList.add(op1);
        operationsList.add(op2);
        objectUnderTest.successfulOperationsHandler(operationsList);
        verify(op1).releaseEventHandle(eq(true));
        verify(op2).releaseEventHandle(eq(true));
    }

    @Test
    void test_successful_records_handling_with_forwarding_pipelines() {
        HeadlessPipeline forwardPipeline = mock(HeadlessPipeline.class);
        when(sinkContext.getForwardToPipelines()).thenReturn(Map.of("fwd_pipeline", forwardPipeline));
        final EventHandle eventHandle = mock(EventHandle.class);
        final BulkIngester objectUnderTest = createObjectUnderTest();
        BulkOperationWrapper op1 = mock(BulkOperationWrapper.class);
        BulkOperationWrapper op2 = mock(BulkOperationWrapper.class);
        Event event = mock(Event.class);
        when(op1.getEvent()).thenReturn(event);
        when(op2.getEvent()).thenReturn(event);
        List<BulkOperationWrapper> operationsList = new ArrayList<>();
        operationsList.add(op1);
        operationsList.add(op2);
        objectUnderTest.successfulOperationsHandler(operationsList);
        verify(eventHandle, times(0)).release(eq(true));
        verify(op1, times(1)).getEvent();
        verify(op2, times(1)).getEvent();
        verify(sinkContext, times(1)).forwardRecords(any(SinkForwardRecordsContext.class), eq(null), eq(null));
    }

    @Test
    void test_successful_records_handling_without_forwarding_pipelines_bulk_operations_with_events() {
        when(sinkContext.getForwardToPipelines()).thenReturn(Map.of());
        final EventHandle eventHandle = mock(EventHandle.class);
        final BulkIngester objectUnderTest = createObjectUnderTest();
        BulkOperationWrapper op1 = mock(BulkOperationWrapper.class);
        BulkOperationWrapper op2 = mock(BulkOperationWrapper.class);
        Event event = mock(Event.class);
        when(event.getEventHandle()).thenReturn(eventHandle);
        when(op1.getEvent()).thenReturn(event);
        when(op2.getEvent()).thenReturn(event);
        List<BulkOperationWrapper> operationsList = new ArrayList<>();
        operationsList.add(op1);
        operationsList.add(op2);
        objectUnderTest.successfulOperationsHandler(operationsList);
        verify(eventHandle, times(2)).release(eq(true));
        verify(op1, times(0)).getEventHandle();
        verify(op2, times(0)).getEventHandle();
    }

    @Test
    void output_with_invalid_version_expression_catches_NumberFormatException_and_creates_DLQObject() throws IOException {
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

        final BulkIngester objectUnderTest = createObjectUnderTest();
        initializeIngester(objectUnderTest);
        when(indexManager.getIndexName(anyString())).thenReturn(index);

        final DlqObject dlqObject = mock(DlqObject.class);
        final DlqObject.Builder dlqObjectBuilder = mock(DlqObject.Builder.class);
        final ArgumentCaptor<FailedDlqData> failedDlqData = ArgumentCaptor.forClass(FailedDlqData.class);
        when(dlqObjectBuilder.withEventHandle(eventHandle)).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withFailedData(failedDlqData.capture())).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPluginName("opensearch")).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPluginId("opensearch")).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPipelineName(pipeline)).thenReturn(dlqObjectBuilder);

        when(dlqObject.getFailedData()).thenReturn(mock(FailedDlqData.class));
        doNothing().when(dlqObject).releaseEventHandle(false);
        when(dlqObjectBuilder.build()).thenReturn(dlqObject);

        try (final MockedStatic<DocumentBuilder> documentBuilderMockedStatic = mockStatic(DocumentBuilder.class);
             final MockedStatic<DlqObject> dlqObjectMockedStatic = mockStatic(DlqObject.class)) {
            documentBuilderMockedStatic.when(() -> DocumentBuilder.build(eq(event), eq(null), eq(null), eq(null), eq(null)))
                    .thenReturn(UUID.randomUUID().toString());
            dlqObjectMockedStatic.when(DlqObject::builder).thenReturn(dlqObjectBuilder);
            objectUnderTest.output(List.of(eventRecord));
        }

        final FailedDlqData failedDlqDataResult = failedDlqData.getValue();
        assertThat(failedDlqDataResult, notNullValue());
        assertThat(failedDlqDataResult.getDocument(), equalTo(document));
        assertThat(failedDlqDataResult.getIndex(), equalTo(index));
        assertThat(failedDlqDataResult.getMessage().startsWith("Unable to convert the result of evaluating document_version"), equalTo(true));

        verify(dynamicDocumentVersionDroppedEvents).increment();
    }

    @ParameterizedTest
    @MethodSource("invalidVersionExceptionProvider")
    void output_with_invalid_version_expression_does_not_add_event_to_bulk_request(
            final Class<? extends RuntimeException> exceptionType) throws IOException {
        final String versionExpression = UUID.randomUUID().toString();
        when(indexConfiguration.getVersionExpression()).thenReturn(versionExpression);

        final Event event = mock(JacksonEvent.class);
        final String document = UUID.randomUUID().toString();
        when(event.toJsonString()).thenReturn(document);
        final EventHandle eventHandle = mock(EventHandle.class);
        when(event.getEventHandle()).thenReturn(eventHandle);
        final String index = UUID.randomUUID().toString();
        when(event.formatString(versionExpression, expressionEvaluator)).thenThrow(exceptionType);
        when(event.formatString(indexConfiguration.getIndexAlias(), expressionEvaluator)).thenReturn(index);
        final Record<Event> eventRecord = new Record<>(event);

        final BulkIngester objectUnderTest = createObjectUnderTest();
        initializeIngester(objectUnderTest);
        when(indexManager.getIndexName(anyString())).thenReturn(index);

        final DlqObject dlqObject = mock(DlqObject.class);
        final DlqObject.Builder dlqObjectBuilder = mock(DlqObject.Builder.class);
        when(dlqObjectBuilder.withEventHandle(eventHandle)).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withFailedData(any(FailedDlqData.class))).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPluginName("opensearch")).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPluginId("opensearch")).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPipelineName(pipeline)).thenReturn(dlqObjectBuilder);
        when(dlqObject.getFailedData()).thenReturn(mock(FailedDlqData.class));
        doNothing().when(dlqObject).releaseEventHandle(false);
        when(dlqObjectBuilder.build()).thenReturn(dlqObject);

        try (final MockedStatic<DocumentBuilder> documentBuilderMockedStatic = mockStatic(DocumentBuilder.class);
             final MockedStatic<DlqObject> dlqObjectMockedStatic = mockStatic(DlqObject.class)) {
            documentBuilderMockedStatic.when(() -> DocumentBuilder.build(eq(event), eq(null), eq(null), eq(null), eq(null)))
                    .thenReturn(UUID.randomUUID().toString());
            dlqObjectMockedStatic.when(DlqObject::builder).thenReturn(dlqObjectBuilder);
            objectUnderTest.output(List.of(eventRecord));
        }

        verify(dynamicDocumentVersionDroppedEvents).increment();
        verify(event, times(0)).getJsonNode();
    }

    private static Stream<Arguments> invalidVersionExceptionProvider() {
        return Stream.of(
                Arguments.of(NumberFormatException.class),
                Arguments.of(RuntimeException.class)
        );
    }

    @Test
    void test_routing_field_in_document() {
        String routingFieldKey = UUID.randomUUID().toString();
        String routingKey = UUID.randomUUID().toString();
        String routingFieldValue = UUID.randomUUID().toString();
        when(indexConfiguration.getRoutingField()).thenReturn(routingFieldKey);
        when(indexConfiguration.getRouting()).thenReturn(routingKey);
        final BulkIngester objectUnderTest = createObjectUnderTest();
        final Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(Collections.singletonMap(routingFieldKey, routingFieldValue))
                .build();
        assertThat(objectUnderTest.getDocument(event).getRoutingField(), equalTo(Optional.of(routingFieldValue)));
    }

    @Test
    void test_routing_in_document() {
        String routingValue = UUID.randomUUID().toString();
        String routingKey = UUID.randomUUID().toString();
        final BulkIngester objectUnderTest = createObjectUnderTest();
        final Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(Collections.singletonMap(routingKey, routingValue))
                .build();
        assertThat(objectUnderTest.getDocument(event).getRoutingField(), equalTo(Optional.empty()));

        when(indexConfiguration.getRouting()).thenReturn("${" + routingKey + "}");
        final BulkIngester objectUnderTest2 = createObjectUnderTest();
        assertThat(objectUnderTest2.getDocument(event).getRoutingField(), equalTo(Optional.of(routingValue)));
    }

    @Test
    void output_with_invalid_version_expression_result_catches_RuntimeException_and_creates_DLQObject() throws IOException {
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

        final BulkIngester objectUnderTest = createObjectUnderTest();
        initializeIngester(objectUnderTest);
        when(indexManager.getIndexName(anyString())).thenReturn(index);

        final DlqObject dlqObject = mock(DlqObject.class);
        final DlqObject.Builder dlqObjectBuilder = mock(DlqObject.Builder.class);
        final ArgumentCaptor<FailedDlqData> failedDlqData = ArgumentCaptor.forClass(FailedDlqData.class);
        when(dlqObjectBuilder.withEventHandle(eventHandle)).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withFailedData(failedDlqData.capture())).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPluginName("opensearch")).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPluginId("opensearch")).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPipelineName(pipeline)).thenReturn(dlqObjectBuilder);

        when(dlqObject.getFailedData()).thenReturn(mock(FailedDlqData.class));
        doNothing().when(dlqObject).releaseEventHandle(false);
        when(dlqObjectBuilder.build()).thenReturn(dlqObject);

        try (final MockedStatic<DocumentBuilder> documentBuilderMockedStatic = mockStatic(DocumentBuilder.class);
             final MockedStatic<DlqObject> dlqObjectMockedStatic = mockStatic(DlqObject.class)) {
            documentBuilderMockedStatic.when(() -> DocumentBuilder.build(eq(event), eq(null), eq(null), eq(null), eq(null)))
                    .thenReturn(UUID.randomUUID().toString());
            dlqObjectMockedStatic.when(DlqObject::builder).thenReturn(dlqObjectBuilder);
            objectUnderTest.output(List.of(eventRecord));
        }

        final FailedDlqData failedDlqDataResult = failedDlqData.getValue();
        assertThat(failedDlqDataResult, notNullValue());
        assertThat(failedDlqDataResult.getDocument(), equalTo(document));
        assertThat(failedDlqDataResult.getIndex(), equalTo(index));
        assertThat(failedDlqDataResult.getMessage().startsWith("There was an exception when evaluating the document_version"), equalTo(true));

        verify(dynamicDocumentVersionDroppedEvents).increment();
    }

    @Test
    void createDlqObjectFromEvent_with_null_message_uses_default_message() throws Exception {
        final Event event = mock(JacksonEvent.class);
        final String document = UUID.randomUUID().toString();
        when(event.toJsonString()).thenReturn(document);
        final EventHandle eventHandle = mock(EventHandle.class);
        when(event.getEventHandle()).thenReturn(eventHandle);
        final String index = UUID.randomUUID().toString();

        final BulkIngester objectUnderTest = createObjectUnderTest();

        final DlqObject.Builder dlqObjectBuilder = mock(DlqObject.Builder.class);
        final ArgumentCaptor<FailedDlqData> failedDlqData = ArgumentCaptor.forClass(FailedDlqData.class);
        when(dlqObjectBuilder.withEventHandle(eventHandle)).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withFailedData(failedDlqData.capture())).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPluginName("opensearch")).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPluginId("opensearch")).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.withPipelineName(pipeline)).thenReturn(dlqObjectBuilder);
        when(dlqObjectBuilder.build()).thenReturn(mock(DlqObject.class));

        try (final MockedStatic<DlqObject> dlqObjectMockedStatic = mockStatic(DlqObject.class)) {
            dlqObjectMockedStatic.when(DlqObject::builder).thenReturn(dlqObjectBuilder);

            java.lang.reflect.Method method = BulkIngester.class.getDeclaredMethod("createDlqObjectFromEvent", Event.class, String.class, String.class);
            method.setAccessible(true);
            method.invoke(objectUnderTest, event, index, null);
        }

        final FailedDlqData failedDlqDataResult = failedDlqData.getValue();
        assertThat(failedDlqDataResult, notNullValue());
        assertThat(failedDlqDataResult.getDocument(), equalTo(document));
        assertThat(failedDlqDataResult.getIndex(), equalTo(index));
        assertThat(failedDlqDataResult.getMessage(), equalTo(""));
    }

    @ParameterizedTest
    @MethodSource("externalVersionTypeProvider")
    void initialize_sets_isExternalVersioning_true_on_BulkRetryStrategy_for_external_version_types(
            final VersionType versionType) throws Exception {
        when(indexConfiguration.getVersionType()).thenReturn(versionType);

        final BulkIngester objectUnderTest = createObjectUnderTest();
        initializeIngester(objectUnderTest);

        final BulkRetryStrategy bulkRetryStrategy = getField(objectUnderTest, "bulkRetryStrategy");
        final boolean isExternalVersioning = getField(bulkRetryStrategy, "isExternalVersioning");
        assertThat(isExternalVersioning, equalTo(true));
    }

    @ParameterizedTest
    @MethodSource("nonExternalVersionTypeProvider")
    void initialize_sets_isExternalVersioning_false_on_BulkRetryStrategy_for_non_external_version_types(
            final VersionType versionType) throws Exception {
        lenient().when(indexConfiguration.getVersionType()).thenReturn(versionType);

        final BulkIngester objectUnderTest = createObjectUnderTest();
        initializeIngester(objectUnderTest);

        final BulkRetryStrategy bulkRetryStrategy = getField(objectUnderTest, "bulkRetryStrategy");
        final boolean isExternalVersioning = getField(bulkRetryStrategy, "isExternalVersioning");
        assertThat(isExternalVersioning, equalTo(false));
    }

    private static Stream<Arguments> externalVersionTypeProvider() {
        return Stream.of(
                Arguments.of(VersionType.External),
                Arguments.of(VersionType.ExternalGte)
        );
    }

    private static Stream<Arguments> nonExternalVersionTypeProvider() {
        return Stream.of(
                Arguments.of(VersionType.Internal),
                Arguments.of((VersionType) null)
        );
    }

    private void initializeIngester(final BulkIngester ingester) throws IOException {
        final ConnectionConfiguration connectionConfiguration = mock(ConnectionConfiguration.class);
        lenient().when(connectionConfiguration.isRequestCompressionEnabled()).thenReturn(false);
        lenient().when(openSearchSinkConfiguration.getConnectionConfiguration()).thenReturn(connectionConfiguration);
        lenient().when(indexManager.isIndexAlias(anyString())).thenReturn(false);
        ingester.initialize();
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(final Object target, final String fieldName) throws Exception {
        java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
    }
}
