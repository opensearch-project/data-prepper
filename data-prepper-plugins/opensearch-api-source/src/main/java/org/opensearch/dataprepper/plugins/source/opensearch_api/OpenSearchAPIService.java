/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch_api;

import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.Put;
import org.opensearch.dataprepper.http.common.codec.JsonCodec;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.*;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
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
import org.opensearch.dataprepper.plugins.source.opensearch_api.codec.MultiLineJsonCodec;
import org.opensearch.dataprepper.plugins.source.opensearch_api.model.BulkActionRequest;
import org.opensearch.dataprepper.plugins.source.opensearch_api.model.MetadataKeyAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
* A OpenSearch API Service.
*/
@Blocking
public class OpenSearchAPIService {
    public static final String REQUESTS_RECEIVED = "requestsReceived";
    public static final String SUCCESS_REQUESTS = "successRequests";
    public static final String PAYLOAD_SIZE = "payloadSize";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchAPIService.class);

    // TODO: support other data-types as request body, e.g. json_lines, msgpack
    private final MultiLineJsonCodec jsonCodec = new MultiLineJsonCodec();
    private final Buffer<Record<Event>> buffer;
    private final int bufferWriteTimeoutInMillis;
    private final Counter requestsReceivedCounter;
    private final Counter successRequestsCounter;
    private final DistributionSummary payloadSizeSummary;
    private final Timer requestProcessDuration;

    public OpenSearchAPIService(final int bufferWriteTimeoutInMillis,
                          final Buffer<Record<Event>> buffer,
                                final PluginMetrics pluginMetrics) {
        this.buffer = buffer;
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;

        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
        successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
        payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE);
        requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION);
    }

    @Post("/_bulk")
    @Put
    public HttpResponse doPostBulk(final ServiceRequestContext serviceRequestContext, final AggregatedHttpRequest aggregatedHttpRequest) throws Exception {
        requestsReceivedCounter.increment();
        payloadSizeSummary.record(aggregatedHttpRequest.content().length());

        if(serviceRequestContext.isTimedOut()) {
            return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT);
        }

        return requestProcessDuration.recordCallable(() -> processBulkRequest(aggregatedHttpRequest));
    }

    private HttpResponse processBulkRequest(final AggregatedHttpRequest aggregatedHttpRequest) throws Exception {
        final HttpData content = aggregatedHttpRequest.content();
        List<Map<String, Object>> jsonList;

        try {
            jsonList = jsonCodec.parse(content);
        } catch (IOException e) {
            LOG.error("Failed to parse the request of size {} due to: {}", content.length(), e.getMessage());
            throw new IOException("Bad request data format. Needs to be json array.", e.getCause());
        }
        try {
            if (buffer.isByteBuffer()) {
                // jsonList is ignored in this path but parse() was done to make 
                // sure that the data is in the expected json format
                buffer.writeBytes(content.array(), null, bufferWriteTimeoutInMillis);
            } else {

                List<Record<Event>> records = new ArrayList<>();
                int idx = 0;
                for (; idx<jsonList.size(); idx++) {
                    Map<String, Object> jsonEntry = jsonList.get(idx);
                    BulkActionRequest request = new BulkActionRequest(jsonEntry);
                    boolean isValidBulkAction = Arrays.stream(OpenSearchBulkActions.values())
                            .anyMatch(bulkAction -> bulkAction == OpenSearchBulkActions.fromOptionValue(request.getAction()));
                    if (isValidBulkAction) {

                        final boolean isDeleteAction = request.getAction().equals(OpenSearchBulkActions.DELETE.toString());
                        final JacksonEvent event = isDeleteAction ?
                                JacksonEvent.builder().withEventType(EventType.DOCUMENT.toString()).build() :
                                JacksonEvent.builder().withEventType(EventType.DOCUMENT.toString()).withData(jsonList.get(idx + 1)).build();
                        event.getMetadata().setAttribute(MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE, request.getAction());
                        event.getMetadata().setAttribute(MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE_INDEX, request.getIndex());
                        String docId = request.getId();
                        if (docId != null && !docId.isEmpty() && !docId.isBlank()) {
                            event.getMetadata().setAttribute(MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE_ID, request.getId());
                        }

                        // Skip processing next line
                        if (isDeleteAction) idx++;
                        records.add(new Record<>(event));
                    }
                }
                buffer.writeAll(records, bufferWriteTimeoutInMillis);
            }
        } catch (Exception e) {
            LOG.error("Failed to write the request of size {} due to: {}", content.length(), e.getMessage());
            throw e;
        }
        successRequestsCounter.increment();
        return HttpResponse.of(HttpStatus.OK);
    }

    private Record<Event> buildRecordLog(String json) {
        final JacksonEvent log = JacksonEvent.builder()
                .withData(json)
                .getThis()
                .build();

        return new Record<>(log);
    }

    private boolean isValidBulkAction(String action) {
        return Arrays.stream(OpenSearchBulkActions.values())
                .anyMatch(bulkAction -> bulkAction == OpenSearchBulkActions.fromOptionValue(action));
    }
}
