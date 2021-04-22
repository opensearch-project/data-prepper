/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.sink.opensearch;

import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import io.micrometer.core.instrument.Measurement;
import org.opensearch.OpenSearchException;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.index.Index;
import org.opensearch.index.shard.ShardId;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BulkRetryStrategyTests {
    private static final String PLUGIN_NAME = "opensearch";
    private static final String PIPELINE_NAME = "pipelineName";
    private static final PluginSetting PLUGIN_SETTING = new PluginSetting(PLUGIN_NAME, Collections.emptyMap()) {{
        setPipelineName(PIPELINE_NAME);
    }};
    private static final PluginMetrics PLUGIN_METRICS = PluginMetrics.fromPluginSetting(PLUGIN_SETTING);

    @Before
    public void metricsInit() {
        MetricsTestUtil.initMetrics();
    }

    @Test
    public void testCanRetry() {
        final BulkRetryStrategy bulkRetryStrategy = new BulkRetryStrategy(
                bulkRequest -> new BulkResponse(new BulkItemResponse[bulkRequest.requests().size()], 10),
                (docWriteRequest, throwable) -> {}, PLUGIN_METRICS, BulkRequest::new);
        final String testIndex = "foo";
        final BulkItemResponse bulkItemResponse1 = successItemResponse(testIndex);
        final BulkItemResponse bulkItemResponse2 = badRequestItemResponse(testIndex);
        BulkResponse response = new BulkResponse(
                new BulkItemResponse[]{bulkItemResponse1, bulkItemResponse2}, 10);
        assertFalse(bulkRetryStrategy.canRetry(response));

        final BulkItemResponse bulkItemResponse3 = tooManyRequestItemResponse(testIndex);
        response = new BulkResponse(
                new BulkItemResponse[]{bulkItemResponse1, bulkItemResponse3}, 10);
        assertTrue(bulkRetryStrategy.canRetry(response));

        final BulkItemResponse bulkItemResponse4 = internalServerErrorItemResponse(testIndex);
        response = new BulkResponse(
                new BulkItemResponse[]{bulkItemResponse2, bulkItemResponse4}, 10);
        assertTrue(bulkRetryStrategy.canRetry(response));
    }

    @Test
    public void testExecuteSuccessOnFirstAttempt() throws Exception {
        final String testIndex = "bar";
        final FakeClient client = new FakeClient(testIndex);
        client.successOnFirstAttempt = true;
        final FakeLogger logger = new FakeLogger();

        final BulkRetryStrategy bulkRetryStrategy = new BulkRetryStrategy(
                client::bulk, logger::logFailure, PLUGIN_METRICS, BulkRequest::new);
        final BulkRequest testBulkRequest = new BulkRequest();
        testBulkRequest.add(new IndexRequest(testIndex).id("1"));
        testBulkRequest.add(new IndexRequest(testIndex).id("2"));
        testBulkRequest.add(new IndexRequest(testIndex).id("3"));
        testBulkRequest.add(new IndexRequest(testIndex).id("4"));

        bulkRetryStrategy.execute(testBulkRequest);

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
        final FakeLogger logger = new FakeLogger();

        final BulkRetryStrategy bulkRetryStrategy = new BulkRetryStrategy(
                client::bulk, logger::logFailure, PLUGIN_METRICS, BulkRequest::new);
        final BulkRequest testBulkRequest = new BulkRequest();
        testBulkRequest.add(new IndexRequest(testIndex).id("1"));
        testBulkRequest.add(new IndexRequest(testIndex).id("2"));
        testBulkRequest.add(new IndexRequest(testIndex).id("3"));
        testBulkRequest.add(new IndexRequest(testIndex).id("4"));

        bulkRetryStrategy.execute(testBulkRequest);

        assertEquals(3, client.attempt);
        assertEquals(2, client.finalResponse.getItems().length);
        assertFalse(client.finalResponse.hasFailures());
        assertEquals("3", client.finalRequest.requests().get(0).id());
        assertEquals("4", client.finalRequest.requests().get(1).id());
        final String logging = logger.msg.toString();
        assertTrue(logging.contains("[bar][_doc][2]"));
        assertFalse(logging.contains("[bar][_doc][1]"));

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
        final FakeLogger logger = new FakeLogger();

        final BulkRetryStrategy bulkRetryStrategy = new BulkRetryStrategy(
                client::bulk, logger::logFailure, PLUGIN_METRICS, BulkRequest::new);
        final BulkRequest testBulkRequest = new BulkRequest();
        testBulkRequest.add(new IndexRequest(testIndex).id("1"));
        testBulkRequest.add(new IndexRequest(testIndex).id("2"));
        testBulkRequest.add(new IndexRequest(testIndex).id("3"));
        testBulkRequest.add(new IndexRequest(testIndex).id("4"));

        bulkRetryStrategy.execute(testBulkRequest);

        assertEquals(1, client.attempt);
        final String logging = logger.msg.toString();
        for (int i = 1; i <= 4; i++) {
            assertTrue(logging.contains(String.format("[bar][_doc][%d]", i)));
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
        final FakeLogger logger = new FakeLogger();

        final BulkRetryStrategy bulkRetryStrategy = new BulkRetryStrategy(
                client::bulk, logger::logFailure, PLUGIN_METRICS, BulkRequest::new);
        final BulkRequest testBulkRequest = new BulkRequest();
        testBulkRequest.add(new IndexRequest(testIndex).id("1"));
        testBulkRequest.add(new IndexRequest(testIndex).id("2"));
        testBulkRequest.add(new IndexRequest(testIndex).id("3"));
        testBulkRequest.add(new IndexRequest(testIndex).id("4"));

        bulkRetryStrategy.execute(testBulkRequest);

        assertEquals(1, client.attempt);
        final String logging = logger.msg.toString();
        for (int i = 2; i <= 4; i++) {
            assertTrue(logging.contains(String.format("[bar][_doc][%d]", i)));
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

    private static BulkItemResponse successItemResponse(final String index) {
        final String docId = UUID.randomUUID().toString();
        return new BulkItemResponse(1, DocWriteRequest.OpType.INDEX,
                new IndexResponse(new ShardId(new Index(index, "fakeUUID"), 1),
                        "_doc", docId, 1, 1, 1, true));
    }

    private static BulkItemResponse badRequestItemResponse(final String index) {
        final String docId = UUID.randomUUID().toString();
        return new BulkItemResponse(1, DocWriteRequest.OpType.INDEX,
                new BulkItemResponse.Failure(index, "_doc", docId,
                        new IllegalArgumentException()));
    }

    private static BulkItemResponse tooManyRequestItemResponse(final String index) {
        final String docId = UUID.randomUUID().toString();
        return new BulkItemResponse(1, DocWriteRequest.OpType.INDEX,
                new BulkItemResponse.Failure(index, "_doc", docId,
                        new OpenSearchRejectedExecutionException()));
    }

    private static BulkItemResponse internalServerErrorItemResponse(final String index) {
        final String docId = UUID.randomUUID().toString();
        return new BulkItemResponse(1, DocWriteRequest.OpType.INDEX,
                new BulkItemResponse.Failure(index, "_doc", docId,
                        new IllegalAccessException()));
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

        public BulkResponse bulk(final BulkRequest bulkRequest) throws IOException {
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
            final int requestSize = bulkRequest.requests().size();
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
            final int requestSize = bulkRequest.requests().size();
            assert requestSize == 4;
            final BulkItemResponse[] bulkItemResponses = new BulkItemResponse[]{
                    successItemResponse(index), badRequestItemResponse(index), internalServerErrorItemResponse(index),
                    tooManyRequestItemResponse(index)};
            return new BulkResponse(bulkItemResponses, 10);
        }

        private BulkResponse bulkSecondResponse(final BulkRequest bulkRequest) {
            final int requestSize = bulkRequest.requests().size();
            assert requestSize == 2;
            final BulkItemResponse[] bulkItemResponses = new BulkItemResponse[]{
                    successItemResponse(index), successItemResponse(index)};
            return new BulkResponse(bulkItemResponses, 10);
        }

        private BulkResponse bulkNonRetryableResponse(final BulkRequest bulkRequest) {
            final int requestSize = bulkRequest.requests().size();
            assert requestSize == 4;
            final BulkItemResponse[] bulkItemResponses = new BulkItemResponse[]{
                    successItemResponse(index), badRequestItemResponse(index), badRequestItemResponse(index),
                    badRequestItemResponse(index)};
            return new BulkResponse(bulkItemResponses, 10);
        }

        private BulkResponse bulkSuccessResponse(final BulkRequest bulkRequest) {
            final int requestSize = bulkRequest.requests().size();
            assert requestSize == 4;
            final BulkItemResponse[] bulkItemResponses = new BulkItemResponse[]{
                    successItemResponse(index), successItemResponse(index), successItemResponse(index),
                    successItemResponse(index)};
            return new BulkResponse(bulkItemResponses, 10);
        }
    }

    private static class FakeLogger {
        StringBuilder msg = new StringBuilder();

        public void logFailure(final DocWriteRequest<?> docWriteRequest, final Throwable t) {
            msg.append(String.format("Document [%s] has failure: %s", docWriteRequest.toString(), t));
        }
    }
}
