/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearchapi;

import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.http.BaseHttpService;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.opensearchapi.model.BulkAPIRequestParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/*
 * OpenSearch API Service class is responsible for handling bulk API requests.
 * The bulk API is responsible for 1/ parsing the request body, 2/ validating against the schema for Document API (Bulk) and finally creating data prepper events.
 * Bulk API supports query parameters "pipeline", "routing" and "refresh"
 */
@Blocking
public class OpenSearchAPIService implements BaseHttpService {

    //TODO: Will need to revisit the metrics per API endpoint
    public static final String REQUESTS_RECEIVED = "RequestsReceived";
    public static final String SUCCESS_REQUESTS = "SuccessRequests";
    public static final String PAYLOAD_SIZE = "PayloadSize";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchAPIService.class);

    private final OpenSearchBulkByteDecoder bulkByteDecoder = new OpenSearchBulkByteDecoder();
    private final Buffer<Record<Event>> buffer;
    private final int bufferWriteTimeoutInMillis;
    private final Counter requestsReceivedCounter;
    private final Counter successRequestsCounter;
    private final DistributionSummary payloadSizeSummary;
    private final Timer requestProcessDuration;

    public OpenSearchAPIService(final int bufferWriteTimeoutInMillis, final Buffer<Record<Event>> buffer, final PluginMetrics pluginMetrics) {
        this.buffer = buffer;
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;

        //TODO: Will need to revisit the metrics per API endpoint
        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
        successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
        payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE);
        requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION);
    }

    @Post("/_bulk")
    public HttpResponse doPostBulk(final ServiceRequestContext serviceRequestContext, final AggregatedHttpRequest aggregatedHttpRequest,
                                   @Param("pipeline") @Nullable String pipeline,
                                   @Param("routing") @Nullable String routing) throws Exception {

        BulkAPIRequestParams bulkAPIRequestParams = BulkAPIRequestParams.builder()
                .pipeline(pipeline)
                .routing(routing)
                .build();
        return requestProcessDuration.recordCallable(() -> processBulkRequest(serviceRequestContext, aggregatedHttpRequest, bulkAPIRequestParams));
    }

    @Post("/{index}/_bulk")
    public HttpResponse doPostBulkIndex(final ServiceRequestContext serviceRequestContext, final AggregatedHttpRequest aggregatedHttpRequest,
                                        @Param("index") String index,
                                        @Param("pipeline") @Nullable String pipeline,
                                        @Param("routing") @Nullable String routing) throws Exception {
        BulkAPIRequestParams bulkAPIRequestParams = BulkAPIRequestParams.builder()
                .index(index)
                .pipeline(pipeline)
                .routing(routing)
                .build();
        return requestProcessDuration.recordCallable(() -> processBulkRequest(serviceRequestContext, aggregatedHttpRequest, bulkAPIRequestParams));
    }

    private HttpResponse processBulkRequest(final ServiceRequestContext serviceRequestContext, final AggregatedHttpRequest aggregatedHttpRequest, final BulkAPIRequestParams bulkAPIRequestParams) throws Exception {
        requestsReceivedCounter.increment();
        payloadSizeSummary.record(aggregatedHttpRequest.content().length());

        if (serviceRequestContext.isTimedOut()) {
            return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT);
        }

        final HttpData content = aggregatedHttpRequest.content();

        try {
            if (buffer.isByteBuffer()) {
                buffer.writeBytes(content.array(), null, bufferWriteTimeoutInMillis);
            } else {
                List<Record<Event>> records = new ArrayList<>();
                bulkByteDecoder.parse(new ByteArrayInputStream(content.array()), Instant.now(), bulkAPIRequestParams, records::add);
                buffer.writeAll(records, bufferWriteTimeoutInMillis);
            }
        } catch (Exception e) {
            LOG.error("Failed to write the request of size {} due to: {}", content.length(), e.getMessage());
            throw e;
        }
        successRequestsCounter.increment();
        return HttpResponse.of(HttpStatus.OK);
    }
}
