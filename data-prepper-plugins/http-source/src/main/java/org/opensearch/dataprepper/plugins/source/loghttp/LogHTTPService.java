/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import com.linecorp.armeria.server.ServiceRequestContext;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.record.Record;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Post;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.http.codec.JsonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;


/*
* A HTTP service for log ingestion to be executed by BlockingTaskExecutor.
*/
@Blocking
public class LogHTTPService {
    private static final int SERIALIZATION_OVERHEAD = 1024;
    public static final String REQUESTS_RECEIVED = "requestsReceived";
    public static final String SUCCESS_REQUESTS = "successRequests";
    public static final String REQUESTS_OVER_OPTIMAL_SIZE = "requestsOverOptimalSize";
    public static final String REQUESTS_OVER_MAXIMUM_SIZE = "requestsOverMaximumSize";
    public static final String PAYLOAD_SIZE = "payloadSize";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";

    private static final Logger LOG = LoggerFactory.getLogger(LogHTTPService.class);

    // TODO: support other data-types as request body, e.g. json_lines, msgpack
    private final JsonCodec jsonCodec = new JsonCodec();
    private final Buffer<Record<Log>> buffer;
    private final int bufferWriteTimeoutInMillis;
    private final Counter requestsReceivedCounter;
    private final Counter successRequestsCounter;
    private final Counter requestsOverOptimalSizeCounter;
    private final Counter requestsOverMaximumSizeCounter;
    private final DistributionSummary payloadSizeSummary;
    private final Timer requestProcessDuration;
    private Integer bufferMaxRequestLength;
    private Integer bufferOptimalRequestLength;

    public LogHTTPService(final int bufferWriteTimeoutInMillis,
                          final Buffer<Record<Log>> buffer,
                          final PluginMetrics pluginMetrics) {
        this.buffer = buffer;
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
        this.bufferMaxRequestLength = buffer.getMaxRequestSize().isPresent() ? buffer.getMaxRequestSize().get(): null;
        this.bufferOptimalRequestLength = buffer.getOptimalRequestSize().isPresent() ? buffer.getOptimalRequestSize().get(): null;
        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
        successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
        requestsOverOptimalSizeCounter = pluginMetrics.counter(REQUESTS_OVER_OPTIMAL_SIZE);
        requestsOverMaximumSizeCounter = pluginMetrics.counter(REQUESTS_OVER_MAXIMUM_SIZE);
        payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE);
        requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION);
    }

    @Post
    public HttpResponse doPost(final ServiceRequestContext serviceRequestContext, final AggregatedHttpRequest aggregatedHttpRequest) throws Exception {
        requestsReceivedCounter.increment();
        payloadSizeSummary.record(aggregatedHttpRequest.content().length());

        if(serviceRequestContext.isTimedOut()) {
            return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT);
        }

        return requestProcessDuration.recordCallable(() -> processRequest(aggregatedHttpRequest));
    }

    HttpResponse processRequest(final AggregatedHttpRequest aggregatedHttpRequest) throws Exception {
        final HttpData content = aggregatedHttpRequest.content();

        if (buffer.isByteBuffer()) {
            if (bufferMaxRequestLength != null && bufferOptimalRequestLength != null && content.array().length > bufferOptimalRequestLength) {
                jsonCodec.serializeSplit(content, this::writeChunkedBody, bufferOptimalRequestLength - SERIALIZATION_OVERHEAD);
            } else {
                try {
                    jsonCodec.validate(content);
                } catch (IOException e) {
                    LOG.error("Failed to parse the request of size {} due to: {}", content.length(), e.getMessage());
                    throw new IOException("Bad request data format. Needs to be json array.", e.getCause());
                }

                try {
                    buffer.writeBytes(content.array(), null, bufferWriteTimeoutInMillis);
                } catch (Exception e) {
                    LOG.error("Failed to write the request of size {} due to: {}", content.length(), e.getMessage());
                    throw e;
                }
            }
        } else {
            final List<String> jsonList;

            try {
                jsonList = jsonCodec.parse(content);
            } catch (IOException e) {
                LOG.error("Failed to parse the request of size {} due to: {}", content.length(), e.getMessage());
                throw new IOException("Bad request data format. Needs to be json array.", e.getCause());
            }

            final List<Record<Log>> records = jsonList.stream()
                    .map(this::buildRecordLog)
                    .collect(Collectors.toList());

            try {
                buffer.writeAll(records, bufferWriteTimeoutInMillis);
            } catch (Exception e) {
                LOG.error("Failed to write the request of size {} due to: {}", content.length(), e.getMessage());
                throw e;
            }
        }

        successRequestsCounter.increment();
        return HttpResponse.of(HttpStatus.OK);
    }

    private void writeChunkedBody(final String chunk) {
        final byte[] chunkBytes = chunk.getBytes();

        if (chunkBytes.length > bufferMaxRequestLength) {
            requestsOverMaximumSizeCounter.increment();
            LOG.error("Unable to write chunked bytes of size {} as it exceeds the maximum buffer size of {}", chunkBytes.length, bufferMaxRequestLength);
            return;
        } else if (chunkBytes.length > bufferOptimalRequestLength) {
            requestsOverOptimalSizeCounter.increment();
        }

        final String key = UUID.randomUUID().toString();
        try {
            buffer.writeBytes(chunkBytes, key, bufferWriteTimeoutInMillis);
        } catch (final Exception e) {
            LOG.error("Failed to write chunked bytes of size {} due to: {}", chunkBytes.length, e.getMessage());
        }
    }

    private Record<Log> buildRecordLog(String json) {

        final JacksonLog log = JacksonLog.builder()
                .withData(json)
                .getThis()
                .build();

        return new Record<>(log);
    }
}
