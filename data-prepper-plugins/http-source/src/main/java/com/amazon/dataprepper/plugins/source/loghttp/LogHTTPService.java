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

package com.amazon.dataprepper.plugins.source.loghttp;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.source.loghttp.codec.JsonCodec;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Post;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/*
* A HTTP service for log ingestion to be executed by BlockingTaskExecutor.
*/
@Blocking
public class LogHTTPService {
    public static final String REQUESTS_RECEIVED = "requestsReceived";
    public static final String SUCCESS_REQUESTS = "successRequests";
    public static final String PAYLOAD_SIZE = "payloadSize";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";

    private static final Logger LOG = LoggerFactory.getLogger(LogHTTPService.class);

    // TODO: support other data-types as request body, e.g. json_lines, msgpack
    private final JsonCodec jsonCodec = new JsonCodec();
    private final Buffer<Record<String>> buffer;
    private final int bufferWriteTimeoutInMillis;
    private final RequestExceptionHandler requestExceptionHandler;
    private final Counter requestsReceivedCounter;
    private final Counter successRequestsCounter;
    private final DistributionSummary payloadSizeSummary;
    private final Timer requestProcessDuration;

    public LogHTTPService(final int bufferWriteTimeoutInMillis,
                          final Buffer<Record<String>> buffer,
                          final PluginMetrics pluginMetrics) {
        this.buffer = buffer;
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;

        requestExceptionHandler = new RequestExceptionHandler(pluginMetrics);
        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
        successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
        payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE);
        requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION);
    }

    @Post
    public HttpResponse doPost(final AggregatedHttpRequest aggregatedHttpRequest) {
        return requestProcessDuration.record(() -> processRequest(aggregatedHttpRequest));
    }

    private HttpResponse processRequest(final AggregatedHttpRequest aggregatedHttpRequest) {
        requestsReceivedCounter.increment();

        List<String> jsonList;
        final HttpData content = aggregatedHttpRequest.content();
        payloadSizeSummary.record(content.length());
        try {
            jsonList = jsonCodec.parse(content);
        } catch (IOException e) {
            LOG.error("Failed to write the request content [{}] due to:", content.toStringUtf8(), e);
            return requestExceptionHandler.handleException(e, "Bad request data format. Needs to be json array.");
        }
        final List<Record<String>> records = jsonList.stream().map(Record::new).collect(Collectors.toList());
        try {
            buffer.writeAll(records, bufferWriteTimeoutInMillis);
        } catch (Exception e) {
            LOG.error("Failed to write the request content [{}] due to:", content.toStringUtf8(), e);
            return requestExceptionHandler.handleException(e);
        }
        successRequestsCounter.increment();
        return HttpResponse.of(HttpStatus.OK);
    }
}
