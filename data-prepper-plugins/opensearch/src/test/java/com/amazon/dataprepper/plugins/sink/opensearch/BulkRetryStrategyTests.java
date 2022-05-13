/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink.opensearch;

import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.sink.opensearch.bulk.AccumulatingBulkRequest;
import com.amazon.dataprepper.plugins.sink.opensearch.bulk.JavaClientAccumulatingBulkRequest;
import com.amazon.dataprepper.plugins.sink.opensearch.bulk.SizedJsonData;
import io.micrometer.core.instrument.Measurement;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchException;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BulkRetryStrategyTests {
    private static final String PLUGIN_NAME = "opensearch";
    private static final String PIPELINE_NAME = "pipelineName";
    private static final PluginSetting PLUGIN_SETTING = new PluginSetting(PLUGIN_NAME, Collections.emptyMap()) {{
        setPipelineName(PIPELINE_NAME);
    }};
    private static final PluginMetrics PLUGIN_METRICS = PluginMetrics.fromPluginSetting(PLUGIN_SETTING);
    private BiConsumer<BulkOperation, Throwable> logFailureConsumer;

    @BeforeEach
    public void setUp() {
        MetricsTestUtil.initMetrics();
        logFailureConsumer = mock(BiConsumer.class);
    }

    @Test
    public void testCanRetry() {
        AccumulatingBulkRequest accumulatingBulkRequest = mock(AccumulatingBulkRequest.class);
        final BulkRetryStrategy bulkRetryStrategy = new BulkRetryStrategy(
                bulkRequest -> mock(BulkResponse.class),
                (docWriteRequest, throwable) -> {}, PLUGIN_METRICS, () -> mock(AccumulatingBulkRequest.class));
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

        final BulkRetryStrategy bulkRetryStrategy = new BulkRetryStrategy(
                client::bulk, logFailureConsumer, PLUGIN_METRICS, () -> new JavaClientAccumulatingBulkRequest(new BulkRequest.Builder()));

        final IndexOperation<JsonData> indexOperation1 = new IndexOperation.Builder<JsonData>().index(testIndex).id("1").document(arbitraryDocument()).build();
        final IndexOperation<JsonData> indexOperation2 = new IndexOperation.Builder<JsonData>().index(testIndex).id("2").document(arbitraryDocument()).build();
        final IndexOperation<JsonData> indexOperation3 = new IndexOperation.Builder<JsonData>().index(testIndex).id("3").document(arbitraryDocument()).build();
        final IndexOperation<JsonData> indexOperation4 = new IndexOperation.Builder<JsonData>().index(testIndex).id("4").document(arbitraryDocument()).build();
        final AccumulatingBulkRequest accumulatingBulkRequest = new JavaClientAccumulatingBulkRequest(new BulkRequest.Builder());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation1).build());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation2).build());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation3).build());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation4).build());

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
    }

    @Test
    public void testExecuteRetryable() throws Exception {
        final String testIndex = "bar";
        final FakeClient client = new FakeClient(testIndex);

        final BulkRetryStrategy bulkRetryStrategy = new BulkRetryStrategy(
                client::bulk, logFailureConsumer, PLUGIN_METRICS, () -> new JavaClientAccumulatingBulkRequest(new BulkRequest.Builder()));
        final IndexOperation<JsonData> indexOperation1 = new IndexOperation.Builder<JsonData>().index(testIndex).id("1").document(arbitraryDocument()).build();
        final IndexOperation<JsonData> indexOperation2 = new IndexOperation.Builder<JsonData>().index(testIndex).id("2").document(arbitraryDocument()).build();
        final IndexOperation<JsonData> indexOperation3 = new IndexOperation.Builder<JsonData>().index(testIndex).id("3").document(arbitraryDocument()).build();
        final IndexOperation<JsonData> indexOperation4 = new IndexOperation.Builder<JsonData>().index(testIndex).id("4").document(arbitraryDocument()).build();
        final AccumulatingBulkRequest accumulatingBulkRequest = new JavaClientAccumulatingBulkRequest(new BulkRequest.Builder());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation1).build());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation2).build());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation3).build());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation4).build());

        bulkRetryStrategy.execute(accumulatingBulkRequest);

        assertEquals(3, client.attempt);
        assertEquals(2, client.finalResponse.items().size());
        assertFalse(client.finalResponse.errors());
        assertEquals("3", client.finalRequest.operations().get(0).index().id());
        assertEquals("4", client.finalRequest.operations().get(1).index().id());

        ArgumentCaptor<BulkOperation> loggerWriteRequestArgCaptor = ArgumentCaptor.forClass(BulkOperation.class);
        ArgumentCaptor<Throwable> loggerThrowableArgCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(logFailureConsumer).accept(loggerWriteRequestArgCaptor.capture(), loggerThrowableArgCaptor.capture());
        MatcherAssert.assertThat(loggerWriteRequestArgCaptor.getValue(), notNullValue());
        MatcherAssert.assertThat(loggerWriteRequestArgCaptor.getValue().index().index(), equalTo(testIndex));
        MatcherAssert.assertThat(loggerWriteRequestArgCaptor.getValue().index().id(), equalTo("2"));
        MatcherAssert.assertThat(loggerThrowableArgCaptor.getValue(), notNullValue());

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

        final BulkRetryStrategy bulkRetryStrategy = new BulkRetryStrategy(
                client::bulk, logFailureConsumer, PLUGIN_METRICS, () -> new JavaClientAccumulatingBulkRequest(new BulkRequest.Builder()));
        final IndexOperation<JsonData> indexOperation1 = new IndexOperation.Builder<JsonData>().index(testIndex).id("1").document(arbitraryDocument()).build();
        final IndexOperation<JsonData> indexOperation2 = new IndexOperation.Builder<JsonData>().index(testIndex).id("2").document(arbitraryDocument()).build();
        final IndexOperation<JsonData> indexOperation3 = new IndexOperation.Builder<JsonData>().index(testIndex).id("3").document(arbitraryDocument()).build();
        final IndexOperation<JsonData> indexOperation4 = new IndexOperation.Builder<JsonData>().index(testIndex).id("4").document(arbitraryDocument()).build();
        final AccumulatingBulkRequest accumulatingBulkRequest = new JavaClientAccumulatingBulkRequest(new BulkRequest.Builder());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation1).build());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation2).build());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation3).build());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation4).build());

        bulkRetryStrategy.execute(accumulatingBulkRequest);

        assertEquals(1, client.attempt);

        ArgumentCaptor<BulkOperation> loggerWriteRequestArgCaptor = ArgumentCaptor.forClass(BulkOperation.class);
        ArgumentCaptor<Throwable> loggerExceptionArgCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(logFailureConsumer, times(4))
                .accept(loggerWriteRequestArgCaptor.capture(), isA(IllegalArgumentException.class));
        final List<BulkOperation> allLoggerWriteRequests = loggerWriteRequestArgCaptor.getAllValues();
        for (int i = 0; i < allLoggerWriteRequests.size(); i++) {
            final BulkOperation actualFailedWrite = allLoggerWriteRequests.get(i);
            MatcherAssert.assertThat(actualFailedWrite.index().index(), equalTo(testIndex));
            String expectedIndexName = Integer.toString(i+1);
            MatcherAssert.assertThat(actualFailedWrite.index().id(), equalTo(expectedIndexName));
        }

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

    @Test
    public void testExecuteNonRetryableResponse() throws Exception {
        final String testIndex = "bar";
        final FakeClient client = new FakeClient(testIndex);
        client.retryable = false;
        client.nonRetryableException = false;

        final BulkRetryStrategy bulkRetryStrategy = new BulkRetryStrategy(
                client::bulk, logFailureConsumer, PLUGIN_METRICS, () -> new JavaClientAccumulatingBulkRequest(new BulkRequest.Builder()));
        final IndexOperation<JsonData> indexOperation1 = new IndexOperation.Builder<JsonData>().index(testIndex).id("1").document(arbitraryDocument()).build();
        final IndexOperation<JsonData> indexOperation2 = new IndexOperation.Builder<JsonData>().index(testIndex).id("2").document(arbitraryDocument()).build();
        final IndexOperation<JsonData> indexOperation3 = new IndexOperation.Builder<JsonData>().index(testIndex).id("3").document(arbitraryDocument()).build();
        final IndexOperation<JsonData> indexOperation4 = new IndexOperation.Builder<JsonData>().index(testIndex).id("4").document(arbitraryDocument()).build();
        final AccumulatingBulkRequest accumulatingBulkRequest = new JavaClientAccumulatingBulkRequest(new BulkRequest.Builder());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation1).build());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation2).build());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation3).build());
        accumulatingBulkRequest.addOperation(new BulkOperation.Builder().index(indexOperation4).build());

        bulkRetryStrategy.execute(accumulatingBulkRequest);

        assertEquals(1, client.attempt);

        ArgumentCaptor<BulkOperation> loggerWriteRequestArgCaptor = ArgumentCaptor.forClass(BulkOperation.class);
        ArgumentCaptor<Throwable> loggerExceptionArgCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(logFailureConsumer, times(3))
                .accept(loggerWriteRequestArgCaptor.capture(), isA(RuntimeException.class));
        final List<BulkOperation> allLoggerWriteRequests = loggerWriteRequestArgCaptor.getAllValues();
        for (int i = 0; i < allLoggerWriteRequests.size(); i++) {
            final BulkOperation actualFailedWrite = allLoggerWriteRequests.get(i);
            MatcherAssert.assertThat(actualFailedWrite.index().index(), equalTo(testIndex));
            String expectedIndexName = Integer.toString(i+2);
            MatcherAssert.assertThat(actualFailedWrite.index().id(), equalTo(expectedIndexName));
        }

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


    private static BulkResponseItem successItemResponse(final String index) {
        return mock(BulkResponseItem.class);
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

    private static BulkResponseItem customBulkFailureResponse(final String index, final RestStatus restStatus) {
        final ErrorCause errorCause = mock(ErrorCause.class);
        final BulkResponseItem badResponse = mock(BulkResponseItem.class);
        when(badResponse.status()).thenReturn(restStatus.getStatus());
        when(badResponse.error()).thenReturn(errorCause);
        return badResponse;
    }

    private JsonData arbitraryDocument() {
        return SizedJsonData.fromString("{}", new JacksonJsonpMapper());
    }

    private static class FakeClient {

        boolean successOnFirstAttempt = false;
        boolean retryable = true;
        boolean nonRetryableException = true;
        int attempt = 0;
        String index;
        BulkRequest finalRequest;
        BulkResponse finalResponse;

        public FakeClient(final String index) {
            this.index = index;
        }

        public BulkResponse bulk(final AccumulatingBulkRequest<BulkOperation, BulkRequest> accumulatingBulkRequest) throws IOException {
            final BulkRequest bulkRequest = accumulatingBulkRequest.getRequest();
            if (successOnFirstAttempt) {
                attempt++;
                return bulkSuccessResponse(bulkRequest);
            }
            if (!retryable) {
                attempt++;
                if (nonRetryableException) {
                    throw new IllegalArgumentException();
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
                throw new OpenSearchException(new OpenSearchRejectedExecutionException());
            } else if (attempt == 2) {
                attempt++;
                throw new IOException();
            } else {
                assert requestSize == 2;
                finalResponse = bulkSecondResponse(bulkRequest);
                return finalResponse;
            }
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
