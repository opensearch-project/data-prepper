/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Post;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.http.BaseHttpService;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * HTTP service for handling Prometheus Remote Write requests.
 * This service receives POST requests with Snappy-compressed protobuf payloads,
 * parses them, and writes the resulting metric events to the buffer.
 */
@Blocking
public class PrometheusRemoteWriteService implements BaseHttpService {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusRemoteWriteService.class);

    public static final String REQUESTS_RECEIVED = "requestsReceived";
    public static final String SUCCESS_REQUESTS = "successRequests";
    public static final String FAILED_REQUESTS = "failedRequests";
    public static final String TIMEOUT_REQUESTS = "timeoutRequests";
    public static final String PAYLOAD_SIZE = "payloadSize";
    public static final String RECORDS_CREATED = "recordsCreated";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";

    private final Buffer<Record<Event>> buffer;
    private final int bufferWriteTimeoutInMillis;
    private final RemoteWriteProtobufParser protobufParser;
    private final Counter requestsReceivedCounter;
    private final Counter successRequestsCounter;
    private final Counter failedRequestsCounter;
    private final Counter timeoutRequestsCounter;
    private final Counter recordsCreatedCounter;
    private final DistributionSummary payloadSizeSummary;
    private final Timer requestProcessDuration;

    public PrometheusRemoteWriteService(final int bufferWriteTimeoutInMillis,
                                         final Buffer<Record<Event>> buffer,
                                         final PluginMetrics pluginMetrics,
                                         final RemoteWriteProtobufParser protobufParser) {
        this.buffer = buffer;
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
        this.protobufParser = protobufParser;

        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
        successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
        failedRequestsCounter = pluginMetrics.counter(FAILED_REQUESTS);
        timeoutRequestsCounter = pluginMetrics.counter(TIMEOUT_REQUESTS);
        recordsCreatedCounter = pluginMetrics.counter(RECORDS_CREATED);
        payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE);
        requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION);
    }

    /**
     * Handles POST requests for Prometheus Remote Write.
     *
     * @param serviceRequestContext the request context
     * @param aggregatedHttpRequest the aggregated HTTP request
     * @return HTTP response indicating success or failure
     * @throws Exception if processing fails
     */
    @Post
    public HttpResponse doPost(final ServiceRequestContext serviceRequestContext,
                               final AggregatedHttpRequest aggregatedHttpRequest) throws Exception {
        requestsReceivedCounter.increment();
        payloadSizeSummary.record(aggregatedHttpRequest.content().length());

        if (serviceRequestContext.isTimedOut()) {
            LOG.warn("Request timed out before processing");
            timeoutRequestsCounter.increment();
            return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT);
        }

        final MediaType contentType = aggregatedHttpRequest.contentType();
        if (contentType == null || !isValidContentType(contentType)) {
            LOG.warn("Invalid content type: {}. Expected application/x-protobuf", contentType);
            failedRequestsCounter.increment();
            return HttpResponse.of(HttpStatus.UNSUPPORTED_MEDIA_TYPE,
                    MediaType.PLAIN_TEXT,
                    "Expected Content-Type: application/x-protobuf");
        }

        return requestProcessDuration.recordCallable(() -> processRequest(aggregatedHttpRequest));
    }

    /**
     * Processes the Remote Write request by parsing the protobuf and writing to buffer.
     *
     * @param aggregatedHttpRequest the HTTP request
     * @return HTTP response
     * @throws Exception if processing fails
     */
    private HttpResponse processRequest(final AggregatedHttpRequest aggregatedHttpRequest) throws Exception {
        final HttpData content = aggregatedHttpRequest.content();

        if (content.isEmpty()) {
            LOG.warn("Received empty request body");
            failedRequestsCounter.increment();
            return HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.PLAIN_TEXT, "Empty request body");
        }

        final byte[] decompressed;
        try {
            decompressed = SnappyDecompressor.decompress(content.array());
        } catch (final IOException e) {
            LOG.error("Failed to decompress Snappy payload: {}", e.getMessage());
            failedRequestsCounter.increment();
            return HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.PLAIN_TEXT,
                    "Failed to decompress payload");
        }

        final List<Record<Event>> records;
        try {
            records = protobufParser.parseDecompressed(decompressed);
        } catch (final PrometheusParseException e) {
            LOG.error("Failed to parse Prometheus Remote Write request: {}", e.getMessage());
            failedRequestsCounter.increment();
            return HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.PLAIN_TEXT,
                    e.getMessage());
        } catch (final Exception e) {
            LOG.error("Unexpected error parsing Prometheus Remote Write request: {}", e.getMessage());
            failedRequestsCounter.increment();
            return HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.PLAIN_TEXT,
                    "Failed to parse request");
        }

        if (records.isEmpty()) {
            LOG.debug("No records created from request");
            successRequestsCounter.increment();
            return HttpResponse.of(HttpStatus.NO_CONTENT);
        }

        try {
            buffer.writeAll(records, bufferWriteTimeoutInMillis);
            recordsCreatedCounter.increment(records.size());
            successRequestsCounter.increment();
            LOG.debug("Successfully wrote {} records to buffer", records.size());
            return HttpResponse.of(HttpStatus.OK);
        } catch (Exception e) {
            LOG.error("Failed to write {} records to buffer: {}", records.size(), e.getMessage());
            failedRequestsCounter.increment();
            return HttpResponse.of(HttpStatus.SERVICE_UNAVAILABLE, MediaType.PLAIN_TEXT, "Service temporarily unavailable");
        }
    }

    /**
     * Validates that the content type is application/x-protobuf.
     *
     * @param contentType the content type to validate
     * @return true if valid
     */
    private static boolean isValidContentType(final MediaType contentType) {
        return contentType.is(MediaType.PROTOBUF) ||
                contentType.is(MediaType.X_PROTOBUF);
    }
}
