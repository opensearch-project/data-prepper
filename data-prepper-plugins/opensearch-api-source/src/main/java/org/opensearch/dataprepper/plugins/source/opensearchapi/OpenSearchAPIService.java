/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearchapi;

import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.Param;
import io.micrometer.common.util.StringUtils;
import org.opensearch.dataprepper.http.codec.MultiLineJsonCodec;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
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
import org.opensearch.dataprepper.plugins.source.opensearchapi.model.BulkAPIRequestParams;
import org.opensearch.dataprepper.plugins.source.opensearchapi.model.BulkActionAndMetadataObject;
import org.opensearch.dataprepper.plugins.source.opensearchapi.model.BulkAPIEventMetadataKeyAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.ArrayList;

/*
* OpenSearch API Service class is responsible for handling bulk API requests.
* The bulk API is responsible for 1/ parsing the request body, 2/ validating against the schema for Document API (Bulk) and finally creating data prepper events.
* Bulk API supports query parameters "pipeline", "routing" and "refresh"
*/
@Blocking
public class OpenSearchAPIService {

    //TODO: Will need to revisit the metrics per API endpoint
    public static final String REQUESTS_RECEIVED = "RequestsReceived";
    public static final String SUCCESS_REQUESTS = "SuccessRequests";
    public static final String PAYLOAD_SIZE = "PayloadSize";
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
                                   @Param("pipeline") Optional<String> pipeline, @Param("routing") Optional<String> routing) throws Exception {

        requestsReceivedCounter.increment();
        payloadSizeSummary.record(aggregatedHttpRequest.content().length());

        if(serviceRequestContext.isTimedOut()) {
            return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT);
        }
        BulkAPIRequestParams bulkAPIRequestParams = BulkAPIRequestParams.builder()
                .pipeline(pipeline.orElse(""))
                .routing(routing.orElse(""))
                .build();
        return requestProcessDuration.recordCallable(() -> processBulkRequest(aggregatedHttpRequest, bulkAPIRequestParams));
    }

    @Post("/{index}/_bulk")
    public HttpResponse doPostBulkIndex(final ServiceRequestContext serviceRequestContext, final AggregatedHttpRequest aggregatedHttpRequest, @Param("index") Optional<String> index,
                                    @Param("pipeline") Optional<String> pipeline, @Param("routing") Optional<String> routing) throws Exception {
        requestsReceivedCounter.increment();
        payloadSizeSummary.record(aggregatedHttpRequest.content().length());

        if(serviceRequestContext.isTimedOut()) {
            return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT);
        }
        BulkAPIRequestParams bulkAPIRequestParams = BulkAPIRequestParams.builder()
                .index(index.orElse(""))
                .pipeline(pipeline.orElse(""))
                .routing(routing.orElse(""))
                .build();
        return requestProcessDuration.recordCallable(() -> processBulkRequest(aggregatedHttpRequest, bulkAPIRequestParams));
    }

    private HttpResponse processBulkRequest(final AggregatedHttpRequest aggregatedHttpRequest, final BulkAPIRequestParams bulkAPIRequestParams) throws Exception {
        final HttpData content = aggregatedHttpRequest.content();
        List<Map<String, Object>> bulkRequestPayloadList;

        // parse the request payload
        try {
            bulkRequestPayloadList = jsonCodec.parse(content);
        } catch (IOException e) {
            LOG.error("Failed to parse the request of size {} due to: {}", content.length(), e.getMessage());
            throw new IOException("Bad request data format.", e.getCause());
        }

        try {
            if (buffer.isByteBuffer()) {
                buffer.writeBytes(content.array(), null, bufferWriteTimeoutInMillis);
            } else {
                List<Record<Event>> records = generateEventsFromBulkRequest(bulkRequestPayloadList, bulkAPIRequestParams);
                buffer.writeAll(records, bufferWriteTimeoutInMillis);
            }
        } catch (Exception e) {
            LOG.error("Failed to write the request of size {} due to: {}", content.length(), e.getMessage());
            throw e;
        }
        successRequestsCounter.increment();
        return HttpResponse.of(HttpStatus.OK);
    }

    private boolean isValidBulkAction(Map<String, Object> actionMap) {
        return Arrays.stream(OpenSearchBulkActions.values())
                .anyMatch(bulkAction -> actionMap.containsKey(bulkAction.toString()));
    }

    private List<Record<Event>> generateEventsFromBulkRequest(final List<Map<String, Object>> bulkRequestPayloadList, final BulkAPIRequestParams bulkAPIRequestParams) throws Exception {
        if (bulkRequestPayloadList.isEmpty()) {
            throw new IOException("Invalid request data.");
        }

        List<Record<Event>> records = new ArrayList<>();
        Iterator<Map<String, Object>> bulkRequestPayloadListIterator = bulkRequestPayloadList.iterator();

        while (bulkRequestPayloadListIterator.hasNext()) {
            Map<String, Object> actionMetadataRow = bulkRequestPayloadListIterator.next();
            if (!isValidBulkAction(actionMetadataRow)) {
                throw new IOException("Invalid request data.");
            }

            BulkActionAndMetadataObject bulkActionAndMetadataObject = new BulkActionAndMetadataObject(actionMetadataRow);
            final boolean isDeleteAction = bulkActionAndMetadataObject.getAction().equals(OpenSearchBulkActions.DELETE.toString());
            Optional<Map<String, Object>> documentDataObject = Optional.empty();
            if (!isDeleteAction) {
                if (!bulkRequestPayloadListIterator.hasNext()) {
                    throw new IOException("Invalid request data.");
                }
                documentDataObject = Optional.of(bulkRequestPayloadListIterator.next());
                // Performing another validation check to make sure that the doc row is not a valid action row
                if (!documentDataObject.isPresent() || isValidBulkAction(documentDataObject.get())) {
                    throw new IOException("Invalid request data.");
                }
            }
            final JacksonEvent event = createBulkRequestActionEvent(bulkActionAndMetadataObject, bulkAPIRequestParams, documentDataObject);
            records.add(new Record<>(event));
        }

        return records;
    }

    private JacksonEvent createBulkRequestActionEvent(
          final BulkActionAndMetadataObject bulkActionAndMetadataObject,
          final BulkAPIRequestParams bulkAPIRequestParams, Optional<Map<String, Object>> optionalDocumentData) {

        final JacksonEvent.Builder eventBuilder = JacksonEvent.builder().withEventType(EventType.DOCUMENT.toString());
        optionalDocumentData.ifPresent(eventBuilder::withData);
        final JacksonEvent event = eventBuilder.build();

        final String index = bulkActionAndMetadataObject.getIndex().isBlank() || bulkActionAndMetadataObject.getIndex().isEmpty()
                ? bulkAPIRequestParams.getIndex() : bulkActionAndMetadataObject.getIndex();

        event.getMetadata().setAttribute(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ACTION, bulkActionAndMetadataObject.getAction());
        event.getMetadata().setAttribute(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_INDEX, index);

        String docId = bulkActionAndMetadataObject.getDocId();
        if (!StringUtils.isBlank(docId) && !StringUtils.isEmpty(docId)) {
            event.getMetadata().setAttribute(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ID, docId);
        }

        String pipeline = bulkAPIRequestParams.getPipeline();
        if (!StringUtils.isBlank(pipeline) && !StringUtils.isEmpty(pipeline)) {
            event.getMetadata().setAttribute(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_PIPELINE, pipeline);
        }

        String routing = bulkAPIRequestParams.getRouting();
        if (!StringUtils.isBlank(routing) && !StringUtils.isEmpty(routing)) {
            event.getMetadata().setAttribute(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ROUTING, routing);
        }

        return event;
    }
}
