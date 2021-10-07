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
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Post;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

/*
* A HTTP service for log ingestion to be executed by BlockingTaskExecutor.
*/
@Blocking
public class LogHTTPService {
    public static final String REQUESTS_RECEIVED = "requestsReceived";
    public static final String REQUEST_TIMEOUTS = "requestTimeouts";
    public static final String SUCCESS_REQUESTS = "successRequests";
    public static final String BAD_REQUESTS = "badRequests";
    public static final String PAYLOAD_SUMMARY = "payloadSummary";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";

    // TODO: support other data-types as request body, e.g. json_lines, msgpack
    private final JsonCodec jsonCodec = new JsonCodec();
    private final Buffer<Record<String>> buffer;
    private final int bufferWriteTimeoutInMillis;
    private final Counter requestsReceivedCounter;
    private final Counter requestTimeoutsCounter;
    private final Counter successRequestsCounter;
    private final Counter badRequestsCounter;
    private final DistributionSummary payloadSummary;
    private final Timer requestProcessDuration;

    public LogHTTPService(final int bufferWriteTimeoutInMillis,
                          final Buffer<Record<String>> buffer,
                          final PluginMetrics pluginMetrics) {
        this.buffer = buffer;
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;

        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
        requestTimeoutsCounter = pluginMetrics.counter(REQUEST_TIMEOUTS);
        successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
        badRequestsCounter = pluginMetrics.counter(BAD_REQUESTS);
        payloadSummary = pluginMetrics.summary(PAYLOAD_SUMMARY);
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
        payloadSummary.record(content.length());
        try {
            jsonList = jsonCodec.parse(content);
        } catch (IOException e) {
            badRequestsCounter.increment();
            return HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.ANY_TYPE, "Bad request data format. Needs to be json array.");
        }
        for (String json: jsonList) {
            try {
                // TODO: switch to new API writeAll once ready
                buffer.write(new Record<>(json), bufferWriteTimeoutInMillis);
            } catch (TimeoutException e) {
                requestTimeoutsCounter.increment();
                return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT, MediaType.ANY_TYPE, e.getMessage());
            }
        }
        successRequestsCounter.increment();
        return HttpResponse.of(HttpStatus.OK);
    }
}
