/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import io.micrometer.core.instrument.Measurement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.AccumulatingBulkRequest;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.JavaClientAccumulatingUncompressedBulkRequest;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.SerializedJson;
import org.opensearch.dataprepper.plugins.sink.opensearch.dlq.FailedBulkOperation;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.ExistingDocumentQueryManager;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.BulkRetryStrategy.VERSION_CONFLICT_EXCEPTION_TYPE;

@ExtendWith(MockitoExtension.class)
public class BulkRetryStrategyTests {
    private static final String PLUGIN_NAME = "opensearch";
    private static final String PIPELINE_NAME = "pipelineName";
    private PluginSetting pluginSetting;
    private PluginMetrics pluginMetrics;
    private BiConsumer<List<FailedBulkOperation>, Throwable> logFailureConsumer;
    private int numEventsSucceeded;
    private int numEventsFailed;
    private boolean maxRetriesLimitReached;
    private EventHandle eventHandle1;
    private EventHandle eventHandle2;
    private EventHandle eventHandle3;
    private EventHandle eventHandle4;
    private EventHandle eventHandle5;

    @Mock
    private ExistingDocumentQueryManager existingDocumentQueryManager;

    @BeforeEach
    public void setUp() {
        MetricsTestUtil.initMetrics();
        logFailureConsumer = mock(BiConsumer.class);
        eventHandle1 = mock(EventHandle.class);
        eventHandle2 = mock(EventHandle.class);
        eventHandle3 = mock(EventHandle.class);
        eventHandle4 = mock(EventHandle.class);
        eventHandle5 = mock(EventHandle.class);

        lenient().doAnswer(a -> {
            List<FailedBulkOperation> l = a.getArgument(0);
            numEventsFailed += l.size();
            return null;
        }).when(logFailureConsumer).accept(any(List.class), any(Throwable.class));
        lenient().doAnswer(a -> {
            List<FailedBulkOperation> l = a.getArgument(0);
            numEventsFailed += l.size();
            return null;
        }).when(logFailureConsumer).accept(any(List.class), eq(null));
        pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getName()).thenReturn(PLUGIN_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(PIPELINE_NAME);

        pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);

        final List<EventHandle> eventHandles = Arrays.asList(
            eventHandle1, eventHandle2, eventHandle3, eventHandle4);
        for (final EventHandle eventHandle: eventHandles) {
            lenient().doAnswer(a -> {
                boolean result = a.getArgument(0);
                if (result) {
                    numEventsSucceeded++;
                } else {
                    numEventsFailed++;
                }
                return null;
            }).when(eventHandle).release(any(Boolean.class));
        }
    }

    public BulkRetryStrategy createObjectUnderTest(
        final RequestFunction<AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest>, BulkResponse> requestFunction,
        final BiConsumer<List<FailedBulkOperation>, Throwable> logFailure,
        final Supplier<AccumulatingBulkRequest> bulkRequestSupplier
) {
        return new BulkRetryStrategy(
                requestFunction,
                logFailure,
                pluginMetrics,
                Integer.MAX_VALUE,
                bulkRequestSupplier,
                PIPELINE_NAME,
                PLUGIN_NAME,
                null);
    }

    public BulkRetryStrategy createObjectUnderTest(
        final RequestFunction<AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest>, BulkResponse> requestFunction,
        final BiConsumer<List<FailedBulkOperation>, Throwable> logFailure,
        final int maxRetries,
        final Supplier<AccumulatingBulkRequest> bulkRequestSupplier
) {
        return new BulkRetryStrategy(
                requestFunction,
                logFailure,
                pluginMetrics,
                maxRetries,
                bulkRequestSupplier,
                PIPELINE_NAME,
                PLUGIN_NAME,
                null);
    }

    public BulkRetryStrategy createObjectUnderTest(
            final RequestFunction<AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest>, BulkResponse> requestFunction,
            final BiConsumer<List<FailedBulkOperation>, Throwable> logFailure,
            final int maxRetries,
            final Supplier<AccumulatingBulkRequest> bulkRequestSupplier,
            final ExistingDocumentQueryManager existingDocumentQueryManager
    ) {
        return new BulkRetryStrategy(
                requestFunction,
                logFailure,
                pluginMetrics,
                maxRetries,
                bulkRequestSupplier,
                PIPELINE_NAME,
                PLUGIN_NAME,
                existingDocumentQueryManager);
    }

    @Test
    public void testCanRetry() {
        AccumulatingBulkRequest accumulatingBulkRequest = mock(AccumulatingBulkRequest.class);
        final BulkRetryStrategy bulkRetryStrategy = createObjectUnderTest(
                bulkRequest -> mock(BulkResponse.class),
                (docWriteRequest, throwable) -> {},
                () -> mock(AccumulatingBulkRequest.class));
        final String testIndex = "foo";
        final BulkResponseItem bulkItemResponse1 = successItemResponse(testIndex);
        final BulkResponseItem bulkItemResponse2 = badRequestItemResponse(testIndex);
        BulkResponse bulkResponse = mock(BulkResponse.class);
        when(bulkResponse.items()).thenReturn(Arrays.asList(bulkItemResponse1, bulkItemResponse2));
        assertFalse(bulkRetryStrategy.canRetry(bulkResponse));

        final BulkResponseItem bulkItemResponse3 = tooManyRequestItemResponse(testIndex);
        bulkResponse = mock(BulkResponse.class);
        when(bulkResponse.items()).thenReturn(Arrays.asList(bulkItemResponse1, bulkItemResponse3));
        assertTrue(bulkRetryStrategy.canRetry(bulkResponse));

        final BulkResponseItem bulkItemResponse4 = internalServerErrorItemResponse(testIndex);
        bulkResponse = mock(BulkResponse.class);
        when(bulkResponse.items()).thenReturn(Arrays.asList(bulkItemResponse2, bulkItemResponse4));
        assertTrue(bulkRetryStrategy.canRetry(bulkResponse));
    }

    @Test
    public void testExecuteSuccessOnFirstAttempt() throws Exception {
        final String testIndex = "bar";
        final FakeClient client = new FakeClient(testIndex);
        client.successOnFirstAttempt = true;
        numEventsSucceeded = 0;
        numEventsFailed = 0;

        final BulkRetryStrategy bulkRetryStrategy = createObjectUnderTest(
                client::bulk, logFailureConsumer,
                () -> new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder()));

        final IndexOperation<SerializedJson> indexOperation1 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("1").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation2 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("2").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation3 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("3").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation4 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("4").document(arbitraryDocument()).build();
        final AccumulatingBulkRequest accumulatingBulkRequest = new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder());
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation1).build(), eventHandle1));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation2).build(), eventHandle2));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation3).build(), eventHandle3));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation4).build(), eventHandle4));

        bulkRetryStrategy.execute(accumulatingBulkRequest);

        assertEquals(1, client.attempt);

        // verify metrics
        final List<Measurement> documentsSuccessFirstAttemptMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENTS_SUCCESS_FIRST_ATTEMPT).toString());
        assertEquals(1, documentsSuccessFirstAttemptMeasurements.size());
        assertEquals(4.0, documentsSuccessFirstAttemptMeasurements.get(0).getValue(), 0);
        final List<Measurement> documentsSuccessMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENTS_SUCCESS).toString());
        assertEquals(1, documentsSuccessMeasurements.size());
        assertEquals(4.0, documentsSuccessMeasurements.get(0).getValue(), 0);
        assertEquals(numEventsSucceeded, 4);
    }

    @Test
    public void testExecuteRetryable() throws Exception {
        final String testIndex = "bar";
        final FakeClient client = new FakeClient(testIndex);

        numEventsSucceeded = 0;
        numEventsFailed = 0;
        final BulkRetryStrategy bulkRetryStrategy = createObjectUnderTest(
                client::bulk, logFailureConsumer,
                () -> new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder()));
        final IndexOperation<SerializedJson> indexOperation1 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("1").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation2 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("2").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation3 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("3").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation4 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("4").document(arbitraryDocument()).build();
        final AccumulatingBulkRequest accumulatingBulkRequest = new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder());
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation1).build(), eventHandle1));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation2).build(), eventHandle2));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation3).build(), eventHandle3));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation4).build(), eventHandle4));
        bulkRetryStrategy.execute(accumulatingBulkRequest);

        assertEquals(3, client.attempt);
        assertEquals(2, client.finalResponse.items().size());
        assertFalse(client.finalResponse.errors());
        assertEquals("3", client.finalRequest.operations().get(0).index().id());
        assertEquals("4", client.finalRequest.operations().get(1).index().id());
        assertEquals(3, numEventsSucceeded);
        assertEquals(1, numEventsFailed);

        final ArgumentCaptor<List<FailedBulkOperation>> failedBulkOperationsCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Throwable> throwableArgCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(logFailureConsumer).accept(failedBulkOperationsCaptor.capture(), throwableArgCaptor.capture());
        assertThat(failedBulkOperationsCaptor.getValue(), notNullValue());

        final List<FailedBulkOperation> failedBulkOperations = failedBulkOperationsCaptor.getValue();
        assertThat(failedBulkOperations.size(), equalTo(1));

        final BulkOperationWrapper bulkOperationWithHandle = failedBulkOperations.get(0).getBulkOperation();
        final BulkOperation bulkOperation = bulkOperationWithHandle.getBulkOperation();
        assertThat(bulkOperation.index().index(), equalTo(testIndex));
        assertThat(bulkOperation.index().id(), equalTo("2"));

        // verify metrics
        final List<Measurement> documentsSuccessFirstAttemptMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENTS_SUCCESS_FIRST_ATTEMPT).toString());
        assertEquals(1, documentsSuccessFirstAttemptMeasurements.size());
        assertEquals(1.0, documentsSuccessFirstAttemptMeasurements.get(0).getValue(), 0);
        final List<Measurement> documentsSuccessMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENTS_SUCCESS).toString());
        assertEquals(1, documentsSuccessMeasurements.size());
        assertEquals(3.0, documentsSuccessMeasurements.get(0).getValue(), 0);
        final List<Measurement> documentErrorsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENT_ERRORS).toString());
        assertEquals(1, documentErrorsMeasurements.size());
        assertEquals(1.0, documentErrorsMeasurements.get(0).getValue(), 0);
    }

    @Test
    public void testExecuteNonRetryableException() throws Exception {
        final String testIndex = "bar";
        final FakeClient client = new FakeClient(testIndex);
        client.retryable = false;

        numEventsSucceeded = 0;
        numEventsFailed = 0;
        final BulkRetryStrategy bulkRetryStrategy = createObjectUnderTest(
                client::bulk, logFailureConsumer,
                () -> new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder()));
        final IndexOperation<SerializedJson> indexOperation1 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("1").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation2 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("2").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation3 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("3").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation4 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("4").document(arbitraryDocument()).build();
        final AccumulatingBulkRequest accumulatingBulkRequest = new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder());
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation1).build(), eventHandle1));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation2).build(), eventHandle2));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation3).build(), eventHandle3));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation4).build(), eventHandle4));

        bulkRetryStrategy.execute(accumulatingBulkRequest);

        assertEquals(numEventsSucceeded, 0);
        assertEquals(numEventsFailed, 4);
        assertEquals(1, client.attempt);

        final ArgumentCaptor<List<FailedBulkOperation>> dlqObjectsArgCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Throwable> throwableArgCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(logFailureConsumer)
                .accept(dlqObjectsArgCaptor.capture(), throwableArgCaptor.capture());
        final List<FailedBulkOperation> failedBulkOperations = dlqObjectsArgCaptor.getValue();
        assertThat(failedBulkOperations.size(), equalTo(4));
        AtomicInteger expectedIndexId = new AtomicInteger(1);
        failedBulkOperations.forEach(failedBulkOperation -> {
            final BulkOperationWrapper bulkOperationWithHandle = failedBulkOperation.getBulkOperation();
            final BulkOperation bulkOperation = bulkOperationWithHandle.getBulkOperation();
            assertThat(bulkOperation.index().index(), equalTo(testIndex));
            assertThat(bulkOperation.index().id(), equalTo(String.valueOf(expectedIndexId.get())));
            expectedIndexId.addAndGet(1);
        });

        // verify metrics
        final List<Measurement> documentsSuccessMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENTS_SUCCESS).toString());
        assertEquals(1, documentsSuccessMeasurements.size());
        assertEquals(0.0, documentsSuccessMeasurements.get(0).getValue(), 0);
        final List<Measurement> documentErrorsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENT_ERRORS).toString());
        assertEquals(1, documentErrorsMeasurements.size());
        assertEquals(4.0, documentErrorsMeasurements.get(0).getValue(), 0);
    }

    private void logFailureMaxRetries(final List<FailedBulkOperation> dlqObjects, final Throwable failure) {
        if (failure != null && failure.getMessage().contains("reached the limit of max retries")) {
            maxRetriesLimitReached = true;
            numEventsFailed += dlqObjects.size();
        }
    }

    @Test
    public void testExecuteWithMaxRetries() throws Exception {
        final String testIndex = "bar";
        final FakeClient client = new FakeClient(testIndex);
        final int MAX_RETRIES = 5;

        numEventsSucceeded = 0;
        numEventsFailed = 0;
        maxRetriesLimitReached = false;
        client.maxRetriesTestValue = MAX_RETRIES;
        logFailureConsumer = this::logFailureMaxRetries;
        final BulkRetryStrategy bulkRetryStrategy = createObjectUnderTest(
                client::bulk, logFailureConsumer, MAX_RETRIES,
                () -> new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder()));
        final IndexOperation<SerializedJson> indexOperation1 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("1").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation2 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("2").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation3 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("3").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation4 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("4").document(arbitraryDocument()).build();
        final AccumulatingBulkRequest accumulatingBulkRequest = new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder());
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation1).build(), eventHandle1));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation2).build(), eventHandle2));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation3).build(), eventHandle3));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation4).build(), eventHandle4));
        bulkRetryStrategy.execute(accumulatingBulkRequest);
        assertThat(maxRetriesLimitReached, equalTo(true));
        assertEquals(numEventsSucceeded, 0);
        assertEquals(numEventsFailed, 4);
    }

    @Test
    public void testExecuteWithMaxRetriesWithException() throws Exception {
        final String testIndex = "bar";
        final FakeClient client = new FakeClient(testIndex);
        final int MAX_RETRIES = 5;

        numEventsSucceeded = 0;
        numEventsFailed = 0;
        maxRetriesLimitReached = false;
        client.maxRetriesTestValue = MAX_RETRIES;
        client.maxRetriesWithException = true;
        logFailureConsumer = this::logFailureMaxRetries;
        final BulkRetryStrategy bulkRetryStrategy = createObjectUnderTest(
                client::bulk, logFailureConsumer, MAX_RETRIES,
                () -> new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder()));
        final IndexOperation<SerializedJson> indexOperation1 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("1").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation2 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("2").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation3 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("3").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation4 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("4").document(arbitraryDocument()).build();
        final AccumulatingBulkRequest accumulatingBulkRequest = new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder());
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation1).build(), eventHandle1));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation2).build(), eventHandle2));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation3).build(), eventHandle3));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation4).build(), eventHandle4));
        bulkRetryStrategy.execute(accumulatingBulkRequest);
        assertThat(maxRetriesLimitReached, equalTo(true));
        assertEquals(numEventsSucceeded, 0);
        assertEquals(numEventsFailed, 4);
    }

    @Test
    public void testExecuteWithMaxRetriesAndSuccesses() throws Exception {
        final String testIndex = "bar";
        final FakeClient client = new FakeClient(testIndex);
        final int MAX_RETRIES = 5;

        numEventsSucceeded = 0;
        numEventsFailed = 0;
        maxRetriesLimitReached = false;
        client.maxRetriesTestValue = MAX_RETRIES;
        client.maxRetriesWithSuccesses = true;
        logFailureConsumer = this::logFailureMaxRetries;
        final BulkRetryStrategy bulkRetryStrategy = createObjectUnderTest(
                client::bulk, logFailureConsumer, MAX_RETRIES,
                () -> new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder()));
        final IndexOperation<SerializedJson> indexOperation1 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("1").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation2 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("2").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation3 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("3").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation4 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("4").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation5 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("5").document(arbitraryDocument()).build();
        final AccumulatingBulkRequest accumulatingBulkRequest = new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder());
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation1).build(), eventHandle1));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation2).build(), eventHandle2));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation3).build(), eventHandle3));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation4).build(), eventHandle4));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation5).build(), eventHandle5));
        bulkRetryStrategy.execute(accumulatingBulkRequest);
        assertThat(maxRetriesLimitReached, equalTo(true));
        assertEquals(numEventsSucceeded, 2);
        assertEquals(numEventsFailed, 2);

        final List<Measurement> documentVersionConflictMeasurement = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENTS_VERSION_CONFLICT_ERRORS).toString());
        assertEquals(1, documentVersionConflictMeasurement.size());
        assertEquals(1.0, documentVersionConflictMeasurement.get(0).getValue(), 0);
    }

    @Test
    public void testExecuteNonRetryableResponse() throws Exception {
        final String testIndex = "bar";
        final FakeClient client = new FakeClient(testIndex);
        client.retryable = false;
        client.nonRetryableException = false;

        numEventsSucceeded = 0;
        numEventsFailed = 0;
        final BulkRetryStrategy bulkRetryStrategy = createObjectUnderTest(
                client::bulk, logFailureConsumer,
                () -> new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder()));
        final IndexOperation<SerializedJson> indexOperation1 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("1").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation2 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("2").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation3 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("3").document(arbitraryDocument()).build();
        final IndexOperation<SerializedJson> indexOperation4 = new IndexOperation.Builder<SerializedJson>().index(testIndex).id("4").document(arbitraryDocument()).build();
        final AccumulatingBulkRequest accumulatingBulkRequest = new JavaClientAccumulatingUncompressedBulkRequest(new BulkRequest.Builder());
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation1).build(), eventHandle1));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation2).build(), eventHandle2));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation3).build(), eventHandle3));
        accumulatingBulkRequest.addOperation(new BulkOperationWrapper(new BulkOperation.Builder().index(indexOperation4).build(), eventHandle4));

        bulkRetryStrategy.execute(accumulatingBulkRequest);

        assertEquals(1, client.attempt);
        assertEquals(numEventsSucceeded, 1);
        assertEquals(numEventsFailed, 3);

        final ArgumentCaptor<List<FailedBulkOperation>> failedBulkOperationsCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Throwable> throwableArgCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(logFailureConsumer, times(1))
                .accept(failedBulkOperationsCaptor.capture(), throwableArgCaptor.capture());
        final List<FailedBulkOperation> failedBulkOperations = failedBulkOperationsCaptor.getValue();
        assertThat(failedBulkOperations.size(), equalTo(3));
        AtomicInteger expectedIndexId = new AtomicInteger(1);

        failedBulkOperations.forEach(failedBulkOperation -> {
            expectedIndexId.addAndGet(1);
            final BulkOperationWrapper bulkOperationWithHandle = failedBulkOperation.getBulkOperation();
            final BulkOperation bulkOperation = bulkOperationWithHandle.getBulkOperation();
            assertThat(bulkOperation.index().index(), equalTo(testIndex));
            assertThat(bulkOperation.index().id(), equalTo(String.valueOf(expectedIndexId.get())));
        });

        // verify metrics
        final List<Measurement> documentsSuccessMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENTS_SUCCESS).toString());
        assertEquals(1, documentsSuccessMeasurements.size());
        assertEquals(1.0, documentsSuccessMeasurements.get(0).getValue(), 0);
        final List<Measurement> documentErrorsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENT_ERRORS).toString());
        assertEquals(1, documentErrorsMeasurements.size());
        assertEquals(3.0, documentErrorsMeasurements.get(0).getValue(), 0);
    }

    @Test
    void execute_will_not_send_messages_to_logWriter_when_all_items_fail_with_retryable_status_when_error_is_provided() throws Exception {
        final RequestFunction<AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest>, BulkResponse> requestFunction = mock(RequestFunction.class);
        final Supplier<AccumulatingBulkRequest> bulkRequestSupplier = mock(Supplier.class);

        final int maxRetries = 3;
        final BulkRetryStrategy objectUnderTest = createObjectUnderTest(
                requestFunction, logFailureConsumer, maxRetries,
                bulkRequestSupplier);

        final List<BulkOperationWrapper> operations = new ArrayList<>();
        final List<BulkResponseItem> responseItems = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            final BulkOperationWrapper bulkOperationWrapper = mock(BulkOperationWrapper.class);
            operations.add(bulkOperationWrapper);

            final BulkResponseItem responseItem = mock(BulkResponseItem.class);

            when(responseItem.error()).thenReturn(mock(ErrorCause.class));
            responseItems.add(responseItem);
        }
        final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> bulkRequest = mock(AccumulatingBulkRequest.class);
        when(bulkRequest.getOperationsCount()).thenReturn(operations.size());
        when(bulkRequest.getOperationAt(anyInt())).thenAnswer(a -> operations.get(a.getArgument(0)));

        final BulkResponse allFailingItemsResponse = mock(BulkResponse.class);
        when(allFailingItemsResponse.errors()).thenReturn(true);
        when(allFailingItemsResponse.items()).thenReturn(responseItems);


        final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> reissueRequest = mock(AccumulatingBulkRequest.class);
        when(reissueRequest.getOperationsCount()).thenReturn(operations.size());
        when(reissueRequest.getOperationAt(anyInt())).thenAnswer(a -> operations.get(a.getArgument(0)));
        when(reissueRequest.getOperations()).thenReturn(operations);

        when(requestFunction.apply(bulkRequest)).thenReturn(allFailingItemsResponse);
        when(requestFunction.apply(reissueRequest)).thenReturn(allFailingItemsResponse);

        when(bulkRequestSupplier.get()).thenReturn(reissueRequest);

        objectUnderTest.execute(bulkRequest);

        for (int i = 0; i < operations.size(); i++) {
            final BulkOperationWrapper operation = operations.get(i);
            verify(reissueRequest, times(maxRetries - 1)).addOperation(operation);
            verify(reissueRequest, times(1)).getOperationAt(i);
        }
        verify(reissueRequest, times(maxRetries)).getOperationsCount();
        verify(reissueRequest).getOperations();
        verifyNoMoreInteractions(reissueRequest);

        final ArgumentCaptor<List<FailedBulkOperation>> actualFailedOperationsCaptor = ArgumentCaptor.forClass(List.class);
        verify(logFailureConsumer).accept(actualFailedOperationsCaptor.capture(), any());

        final List<FailedBulkOperation> failedBulkOperations = actualFailedOperationsCaptor.getValue();
        assertThat(failedBulkOperations.size(), equalTo(operations.size()));
        for (int i = 0; i < operations.size(); i++) {
            final FailedBulkOperation failedBulkOperation = failedBulkOperations.get(i);
            final BulkOperationWrapper operation = operations.get(i);

            assertThat(failedBulkOperation, notNullValue());
            assertThat(failedBulkOperation.getBulkOperation(), equalTo(operation));
            assertThat(failedBulkOperation.getFailure(), notNullValue());
        }

        verifyNoMoreInteractions(logFailureConsumer);

        final List<Measurement> documentsDuplicatesMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENTS_DUPLICATES).toString());
        assertThat(documentsDuplicatesMeasurements.size(), equalTo(1));
        assertThat(documentsDuplicatesMeasurements.get(0).getValue(), equalTo(0.0));
    }

    @Test
    void execute_will_not_send_messages_to_logWriter_when_all_items_fail_with_retryable_status_when_no_error() throws Exception {
        final RequestFunction<AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest>, BulkResponse> requestFunction = mock(RequestFunction.class);
        final Supplier<AccumulatingBulkRequest> bulkRequestSupplier = mock(Supplier.class);

        final int maxRetries = 3;
        final BulkRetryStrategy objectUnderTest = createObjectUnderTest(
                requestFunction, logFailureConsumer, maxRetries,
                bulkRequestSupplier);

        final List<BulkOperationWrapper> operations = new ArrayList<>();
        final List<BulkResponseItem> responseItems = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            final BulkOperationWrapper bulkOperationWrapper = mock(BulkOperationWrapper.class);
            operations.add(bulkOperationWrapper);

            final BulkResponseItem responseItem = mock(BulkResponseItem.class);

            when(responseItem.status()).thenReturn(403);
            responseItems.add(responseItem);
        }
        final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> bulkRequest = mock(AccumulatingBulkRequest.class);
        when(bulkRequest.getOperationsCount()).thenReturn(operations.size());
        when(bulkRequest.getOperationAt(anyInt())).thenAnswer(a -> operations.get(a.getArgument(0)));

        final BulkResponse allFailingItemsResponse = mock(BulkResponse.class);
        when(allFailingItemsResponse.errors()).thenReturn(true);
        when(allFailingItemsResponse.items()).thenReturn(responseItems);


        final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> reissueRequest = mock(AccumulatingBulkRequest.class);
        when(reissueRequest.getOperationsCount()).thenReturn(operations.size());
        when(reissueRequest.getOperationAt(anyInt())).thenAnswer(a -> operations.get(a.getArgument(0)));
        when(reissueRequest.getOperations()).thenReturn(operations);

        when(requestFunction.apply(bulkRequest)).thenReturn(allFailingItemsResponse);
        when(requestFunction.apply(reissueRequest)).thenReturn(allFailingItemsResponse);

        when(bulkRequestSupplier.get()).thenReturn(reissueRequest);

        objectUnderTest.execute(bulkRequest);

        for (int i = 0; i < operations.size(); i++) {
            final BulkOperationWrapper operation = operations.get(i);
            verify(reissueRequest, times(maxRetries - 1)).addOperation(operation);
            verify(reissueRequest, times(1)).getOperationAt(i);
        }
        verify(reissueRequest, times(maxRetries)).getOperationsCount();
        verify(reissueRequest).getOperations();
        verifyNoMoreInteractions(reissueRequest);

        final ArgumentCaptor<List<FailedBulkOperation>> actualFailedOperationsCaptor = ArgumentCaptor.forClass(List.class);
        verify(logFailureConsumer).accept(actualFailedOperationsCaptor.capture(), any());

        final List<FailedBulkOperation> failedBulkOperations = actualFailedOperationsCaptor.getValue();
        assertThat(failedBulkOperations.size(), equalTo(operations.size()));
        for (int i = 0; i < operations.size(); i++) {
            final FailedBulkOperation failedBulkOperation = failedBulkOperations.get(i);
            final BulkOperationWrapper operation = operations.get(i);

            assertThat(failedBulkOperation, notNullValue());
            assertThat(failedBulkOperation.getBulkOperation(), equalTo(operation));
            assertThat(failedBulkOperation.getFailure(), notNullValue());
        }

        verifyNoMoreInteractions(logFailureConsumer);

        final List<Measurement> documentsDuplicatesMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENTS_DUPLICATES).toString());
        assertThat(documentsDuplicatesMeasurements.size(), equalTo(1));
        assertThat(documentsDuplicatesMeasurements.get(0).getValue(), equalTo(0.0));
    }

    @Test
    void execute_with_bulk_status_of_potential_duplicates_sends_to_query_manager_and_does_not_retry() throws Exception {
        final RequestFunction<AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest>, BulkResponse> requestFunction = mock(RequestFunction.class);
        final Supplier<AccumulatingBulkRequest> bulkRequestSupplier = mock(Supplier.class);

        final int maxRetries = 3;
        final BulkRetryStrategy objectUnderTest = createObjectUnderTest(
                requestFunction, logFailureConsumer, maxRetries,
                bulkRequestSupplier, existingDocumentQueryManager);

        final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> bulkRequest = mock(AccumulatingBulkRequest.class);
        final List<BulkOperationWrapper> operations = new ArrayList<>();
        final List<BulkResponseItem> responseItems = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            final BulkOperationWrapper bulkOperationWrapper = mock(BulkOperationWrapper.class);
            operations.add(bulkOperationWrapper);

            final BulkResponseItem responseItem = mock(BulkResponseItem.class);
            responseItems.add(responseItem);
        }
        when(bulkRequest.getOperations()).thenReturn(operations);
        when(bulkRequest.getOperationsCount()).thenReturn(operations.size());

        final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> retryBulkRequest = mock(AccumulatingBulkRequest.class);
        when(retryBulkRequest.getOperationsCount()).thenReturn(0);
        when(bulkRequestSupplier.get()).thenReturn(retryBulkRequest);
        final OpenSearchException exception = mock(OpenSearchException.class);
        when(exception.status()).thenReturn(RestStatus.GATEWAY_TIMEOUT.getStatus());
        when(requestFunction.apply(any(AccumulatingBulkRequest.class)))
                .thenThrow(exception);
        objectUnderTest.execute(bulkRequest);

        for (final BulkOperationWrapper bulkOperationWrapper : operations) {
            verify(existingDocumentQueryManager).addBulkOperation(bulkOperationWrapper);
        }

        verifyNoMoreInteractions(requestFunction);
    }

    @Test
    void execute_with_bulk_exception_as_socket_exception_sends_to_query_manager_and_does_not_retry() throws Exception {
        final RequestFunction<AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest>, BulkResponse> requestFunction = mock(RequestFunction.class);
        final Supplier<AccumulatingBulkRequest> bulkRequestSupplier = mock(Supplier.class);

        final int maxRetries = 3;
        final BulkRetryStrategy objectUnderTest = createObjectUnderTest(
                requestFunction, logFailureConsumer, maxRetries,
                bulkRequestSupplier, existingDocumentQueryManager);

        final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> bulkRequest = mock(AccumulatingBulkRequest.class);
        final List<BulkOperationWrapper> operations = new ArrayList<>();
        final List<BulkResponseItem> responseItems = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            final BulkOperationWrapper bulkOperationWrapper = mock(BulkOperationWrapper.class);
            operations.add(bulkOperationWrapper);

            final BulkResponseItem responseItem = mock(BulkResponseItem.class);
            responseItems.add(responseItem);
        }
        when(bulkRequest.getOperations()).thenReturn(operations);
        when(bulkRequest.getOperationsCount()).thenReturn(operations.size());

        final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> retryBulkRequest = mock(AccumulatingBulkRequest.class);
        when(retryBulkRequest.getOperationsCount()).thenReturn(0);
        when(bulkRequestSupplier.get()).thenReturn(retryBulkRequest);
        final SocketTimeoutException exception = mock(SocketTimeoutException.class);
        when(requestFunction.apply(any(AccumulatingBulkRequest.class)))
                .thenThrow(exception);
        objectUnderTest.execute(bulkRequest);

        for (final BulkOperationWrapper bulkOperationWrapper : operations) {
            verify(existingDocumentQueryManager).addBulkOperation(bulkOperationWrapper);
        }

        verifyNoMoreInteractions(requestFunction);
    }

    @Test
    void execute_with_individual_operation_status_of_potential_duplicates_sends_to_query_manager_and_does_not_retry() throws Exception {
        final RequestFunction<AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest>, BulkResponse> requestFunction = mock(RequestFunction.class);
        final Supplier<AccumulatingBulkRequest> bulkRequestSupplier = mock(Supplier.class);

        final int maxRetries = 3;
        final BulkRetryStrategy objectUnderTest = createObjectUnderTest(
                requestFunction, logFailureConsumer, maxRetries,
                bulkRequestSupplier, existingDocumentQueryManager);

        final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> bulkRequest = mock(AccumulatingBulkRequest.class);
        final List<BulkOperationWrapper> operations = new ArrayList<>();
        final List<BulkResponseItem> responseItems = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            final BulkOperationWrapper bulkOperationWrapper = mock(BulkOperationWrapper.class);
            operations.add(bulkOperationWrapper);

            final BulkResponseItem responseItem = mock(BulkResponseItem.class);
            when(responseItem.status()).thenReturn(RestStatus.INTERNAL_SERVER_ERROR.getStatus());
            responseItems.add(responseItem);
        }
        when(bulkRequest.getOperationAt(anyInt())).thenAnswer(a -> operations.get(a.getArgument(0)));
        when(bulkRequest.getOperationsCount()).thenReturn(operations.size());

        final BulkResponse allFailingItemsResponse = mock(BulkResponse.class);
        when(allFailingItemsResponse.errors()).thenReturn(true);
        when(allFailingItemsResponse.items()).thenReturn(responseItems);

        when(requestFunction.apply(bulkRequest))
                .thenReturn(allFailingItemsResponse);

        final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> retryBulkRequest = mock(AccumulatingBulkRequest.class);
        when(retryBulkRequest.getOperationsCount()).thenReturn(0);
        when(bulkRequestSupplier.get()).thenReturn(retryBulkRequest);
        objectUnderTest.execute(bulkRequest);

        for (final BulkOperationWrapper bulkOperationWrapper : operations) {
            verify(existingDocumentQueryManager).addBulkOperation(bulkOperationWrapper);
        }

        verifyNoMoreInteractions(requestFunction);
    }

    @Test
    void execute_with_duplicate_documents_increments_metrics() throws Exception {
        final RequestFunction<AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest>, BulkResponse> requestFunction = mock(RequestFunction.class);
        final Supplier<AccumulatingBulkRequest> bulkRequestSupplier = mock(Supplier.class);

        final int maxRetries = 3;
        final BulkRetryStrategy objectUnderTest = createObjectUnderTest(
                requestFunction, logFailureConsumer, maxRetries,
                bulkRequestSupplier);

        final List<BulkOperationWrapper> operations = new ArrayList<>();
        final List<BulkResponseItem> responseItems = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            final BulkOperationWrapper bulkOperationWrapper = mock(BulkOperationWrapper.class);
            operations.add(bulkOperationWrapper);

            final BulkResponseItem responseItem = mock(BulkResponseItem.class);
            when(responseItem.seqNo()).thenReturn(1L);

            when(responseItem.status()).thenReturn(200);
            responseItems.add(responseItem);
        }
        final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> bulkRequest = mock(AccumulatingBulkRequest.class);
        when(bulkRequest.getOperationsCount()).thenReturn(operations.size());
        when(bulkRequest.getOperationAt(anyInt())).thenAnswer(a -> operations.get(a.getArgument(0)));

        final BulkResponse allFailingItemsResponse = mock(BulkResponse.class);
        when(allFailingItemsResponse.errors()).thenReturn(true);
        when(allFailingItemsResponse.items()).thenReturn(responseItems);


        when(requestFunction.apply(bulkRequest)).thenReturn(allFailingItemsResponse);

        objectUnderTest.execute(bulkRequest);


        final List<Measurement> documentsDuplicatesMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENTS_DUPLICATES).toString());
        assertThat(documentsDuplicatesMeasurements.size(), equalTo(1));
        assertThat(documentsDuplicatesMeasurements.get(0).getValue(), equalTo(5.0));
    }

    private static BulkResponseItem successItemResponse(final String index) {
        final BulkResponseItem response = mock(BulkResponseItem.class);
        lenient().when(response.status()).thenReturn(200);
        return response;
    }

    private static BulkResponseItem badRequestItemResponse(final String index) {
        return customBulkFailureResponse(index, RestStatus.BAD_REQUEST);
    }

    private static BulkResponseItem tooManyRequestItemResponse(final String index) {
        return customBulkFailureResponse(index, RestStatus.TOO_MANY_REQUESTS);
    }

    private static BulkResponseItem internalServerErrorItemResponse(final String index) {
        return customBulkFailureResponse(index, RestStatus.INTERNAL_SERVER_ERROR);
    }

    private static BulkResponseItem versionConflictErrorItemResponse() {
        return customBulkFailureResponse(RestStatus.CONFLICT, VERSION_CONFLICT_EXCEPTION_TYPE);
    }

    private static BulkResponseItem customBulkFailureResponse(final String index, final RestStatus restStatus) {
        final ErrorCause errorCause = mock(ErrorCause.class);
        final BulkResponseItem badResponse = mock(BulkResponseItem.class);
        lenient().when(badResponse.status()).thenReturn(restStatus.getStatus());
        lenient().when(badResponse.error()).thenReturn(errorCause);
        return badResponse;
    }

    private static BulkResponseItem customBulkFailureResponse(final RestStatus restStatus, final String errorType) {
        final ErrorCause errorCause = mock(ErrorCause.class);
        lenient().when(errorCause.type()).thenReturn(errorType);
        final BulkResponseItem badResponse = mock(BulkResponseItem.class);
        lenient().when(badResponse.status()).thenReturn(restStatus.getStatus());
        lenient().when(badResponse.error()).thenReturn(errorCause);
        return badResponse;
    }

    private SerializedJson arbitraryDocument() {
        return SerializedJson.fromStringAndOptionals("{}", null, null, null);
    }

    private static class FakeClient {

        boolean successOnFirstAttempt = false;
        boolean retryable = true;
        boolean nonRetryableException = true;
        boolean maxRetriesWithSuccesses = false;
        boolean maxRetriesWithException = false;
        int maxRetriesTestValue = 0;
        int attempt = 0;
        String index;
        BulkRequest finalRequest;
        BulkResponse finalResponse;

        public FakeClient(final String index) {
            this.index = index;
        }

        public BulkResponse bulk(final AccumulatingBulkRequest<BulkOperationWrapper, BulkRequest> accumulatingBulkRequest) throws IOException {
            final BulkRequest bulkRequest = accumulatingBulkRequest.getRequest();
            if (maxRetriesTestValue > 0) {
                if (maxRetriesWithException) {
                    throw new OpenSearchException(new ErrorResponse.Builder()
                            .status(500)
                            .error(new ErrorCause.Builder().reason("").type("").build()).build());
                }
                if (++attempt > 5) {
                    assert attempt == 0;
                }
                if (maxRetriesWithSuccesses) {
                    if (attempt == 1) {
                        finalResponse = bulkMaxRetriesResponseWithSuccesses(bulkRequest);
                    } else {
                        finalResponse = bulkSecondResponseWithFailures(bulkRequest);
                    }
                } else {
                    finalResponse = bulkMaxRetriesResponse(bulkRequest);
                }
                return finalResponse;
            }
            if (successOnFirstAttempt) {
                attempt++;
                return bulkSuccessResponse(bulkRequest);
            }
            if (!retryable) {
                attempt++;
                if (nonRetryableException) {
                    throw new IllegalArgumentException("Some argument is invalid");
                } else {
                    return bulkNonRetryableResponse(bulkRequest);
                }
            }
            finalRequest = bulkRequest;
            final int requestSize = bulkRequest.operations().size();
            if (attempt == 0) {
                assert requestSize == 4;
                attempt++;
                finalResponse = bulkFirstResponse(bulkRequest);
                return finalResponse;
            } else if (attempt == 1) {
                attempt++;
                throw new OpenSearchException(new ErrorResponse.Builder()
                        .status(500)
                        .error(new ErrorCause.Builder().reason("").type("").build()).build());
            } else if (attempt == 2) {
                attempt++;
                throw new IOException();
            } else {
                assert requestSize == 2;
                finalResponse = bulkSecondResponse(bulkRequest);
                return finalResponse;
            }
        }

        private BulkResponse bulkMaxRetriesResponseWithSuccesses(final BulkRequest bulkRequest) {
            final List<BulkResponseItem> bulkItemResponses = Arrays.asList(
                    internalServerErrorItemResponse(index),
                    successItemResponse(index),
                    successItemResponse(index),
                    tooManyRequestItemResponse(index),
                    versionConflictErrorItemResponse());
            return new BulkResponse.Builder().items(bulkItemResponses).errors(true).took(10).build();
        }

        private BulkResponse bulkMaxRetriesResponse(final BulkRequest bulkRequest) {
            final int requestSize = bulkRequest.operations().size();
            assertThat(requestSize, equalTo(4));
            final List<BulkResponseItem> bulkItemResponses = Arrays.asList(
                    internalServerErrorItemResponse(index),
                    tooManyRequestItemResponse(index),
                    internalServerErrorItemResponse(index),
                    tooManyRequestItemResponse(index));
            return new BulkResponse.Builder().items(bulkItemResponses).errors(true).took(10).build();
        }

        private BulkResponse bulkFirstResponse(final BulkRequest bulkRequest) {
            final int requestSize = bulkRequest.operations().size();
            assert requestSize == 4;
            final List<BulkResponseItem> bulkItemResponses = Arrays.asList(
                    successItemResponse(index), badRequestItemResponse(index), internalServerErrorItemResponse(index),
                    tooManyRequestItemResponse(index));
            return new BulkResponse.Builder().items(bulkItemResponses).errors(true).took(10).build();
        }

        private BulkResponse bulkSecondResponse(final BulkRequest bulkRequest) {
            final int requestSize = bulkRequest.operations().size();
            assert requestSize == 2;
            final List<BulkResponseItem> bulkItemResponses = Arrays.asList(
                    successItemResponse(index), successItemResponse(index));
            return new BulkResponse.Builder().items(bulkItemResponses).errors(false).took(10).build();
        }

        private BulkResponse bulkSecondResponseWithFailures(final BulkRequest bulkRequest) {
            final int requestSize = bulkRequest.operations().size();
            assert requestSize == 2;
            final List<BulkResponseItem> bulkItemResponses = Arrays.asList(
                    internalServerErrorItemResponse(index), internalServerErrorItemResponse(index));
            return new BulkResponse.Builder().items(bulkItemResponses).errors(true).took(10).build();
        }

        private BulkResponse bulkNonRetryableResponse(final BulkRequest bulkRequest) {
            final int requestSize = bulkRequest.operations().size();
            assert requestSize == 4;
            final List<BulkResponseItem> bulkItemResponses = Arrays.asList(
                    successItemResponse(index), badRequestItemResponse(index), badRequestItemResponse(index),
                    badRequestItemResponse(index));
            return new BulkResponse.Builder().items(bulkItemResponses).errors(true).took(10).build();
        }

        private BulkResponse bulkSuccessResponse(final BulkRequest bulkRequest) {
            final int requestSize = bulkRequest.operations().size();
            assert requestSize == 4;
            final List<BulkResponseItem> bulkItemResponses = Arrays.asList(
                    successItemResponse(index), successItemResponse(index), successItemResponse(index),
                    successItemResponse(index));
            return new BulkResponse.Builder().items(bulkItemResponses).took(10).errors(false).build();
        }
    }
}
