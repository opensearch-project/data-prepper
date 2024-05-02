/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch_api;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.*;
import org.opensearch.client.json.jackson.JacksonJsonpGenerator;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
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
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.SearchResponse;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.pipeline.OpenSearchAPI;
import org.opensearch.dataprepper.plugins.source.opensearch_api.codec.MultiLineJsonCodec;
import org.opensearch.dataprepper.plugins.source.opensearch_api.model.BulkAPIRequestParams;
import org.opensearch.dataprepper.plugins.source.opensearch_api.model.BulkActionRequest;
import org.opensearch.dataprepper.plugins.source.opensearch_api.model.MetadataKeyAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.io.InvalidObjectException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

/*
* A OpenSearch API Service.
*/
@Blocking
public class OpenSearchAPIService {
    public static final String REQUESTS_RECEIVED_BULK = "requestsReceivedBulkAPI";
    public static final String SUCCESS_REQUESTS_BULK = "successRequestsBulkAPI";
    public static final String PAYLOAD_SIZE_BULK = "payloadSizeBulkAPI";
    public static final String REQUESTS_RECEIVED_SEARCH = "requestsReceivedSearchAPI";
    public static final String SUCCESS_REQUESTS_SEARCH = "successRequestsSearchAPI";
    public static final String PAYLOAD_SIZE_SEARCH = "payloadSizeSearchAPI";
    public static final String REQUEST_PROCESS_DURATION_SEARCH = "requestProcessDurationSearchAPI";
    public static final String REQUEST_PROCESS_DURATION_BULK = "requestProcessDurationBulkAPI";

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchAPIService.class);

    // TODO: support other data-types as request body, e.g. json_lines, msgpack
    private final MultiLineJsonCodec jsonCodec = new MultiLineJsonCodec();
    private final OpenSearchAPISource source;
    private final Buffer<Record<Event>> buffer;
    private final int bufferWriteTimeoutInMillis;
    private final Counter requestsReceivedCounterBulk;
    private final Counter successRequestsCounterBulk;
    private final DistributionSummary payloadSizeSummaryBulk;
    private final Counter requestsReceivedCounterSearch;
    private final Counter successRequestsCounterSearch;
    private final DistributionSummary payloadSizeSummarySearch;
    private final Timer requestProcessDurationBulkAPI;
    private final Timer requestProcessDurationSearchAPI;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonFactory factory = new JsonFactory();

    public OpenSearchAPIService(final OpenSearchAPISource source, final int bufferWriteTimeoutInMillis,
                          final Buffer<Record<Event>> buffer,
                                final PluginMetrics pluginMetrics) {
        this.source = source;
        this.buffer = buffer;
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;

        requestsReceivedCounterBulk = pluginMetrics.counter(REQUESTS_RECEIVED_BULK);
        successRequestsCounterBulk = pluginMetrics.counter(SUCCESS_REQUESTS_BULK);
        payloadSizeSummaryBulk = pluginMetrics.summary(PAYLOAD_SIZE_BULK);
        requestsReceivedCounterSearch = pluginMetrics.counter(REQUESTS_RECEIVED_SEARCH);
        successRequestsCounterSearch = pluginMetrics.counter(SUCCESS_REQUESTS_SEARCH);
        payloadSizeSummarySearch = pluginMetrics.summary(PAYLOAD_SIZE_SEARCH);

        requestProcessDurationSearchAPI = pluginMetrics.timer(REQUEST_PROCESS_DURATION_SEARCH);
        requestProcessDurationBulkAPI = pluginMetrics.timer(REQUEST_PROCESS_DURATION_BULK);
    }

    @Post("/{index}/_bulk")
    public HttpResponse doPostIndex(final ServiceRequestContext serviceRequestContext, final AggregatedHttpRequest aggregatedHttpRequest, @Param("index") String index) throws Exception {
        requestsReceivedCounterBulk.increment();
        payloadSizeSummaryBulk.record(aggregatedHttpRequest.content().length());

        if(serviceRequestContext.isTimedOut()) {
            return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT);
        }
        BulkAPIRequestParams bulkAPIRequestParams = BulkAPIRequestParams.builder().index(index).build();
        return requestProcessDurationBulkAPI.recordCallable(() -> processBulkRequest(aggregatedHttpRequest, bulkAPIRequestParams));
    }

    @Post("/_bulk")
    @Put
    public HttpResponse doPostUpdate(final ServiceRequestContext serviceRequestContext, final AggregatedHttpRequest aggregatedHttpRequest) throws Exception {
        requestsReceivedCounterBulk.increment();
        payloadSizeSummaryBulk.record(aggregatedHttpRequest.content().length());

        if(serviceRequestContext.isTimedOut()) {
            return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT);
        }

        BulkAPIRequestParams bulkAPIRequestParams = BulkAPIRequestParams.builder().build();
        return requestProcessDurationBulkAPI.recordCallable(() -> processBulkRequest(aggregatedHttpRequest,bulkAPIRequestParams));
    }

    @Get("/{index}/_search")
    public HttpResponse doSearch(final ServiceRequestContext serviceRequestContext, final AggregatedHttpRequest aggregatedHttpRequest, @Param("index") String index) throws Exception {

        if(serviceRequestContext.isTimedOut()) {
            return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT);
        }

        return requestProcessDurationSearchAPI.recordCallable(() -> processSearchRequest(aggregatedHttpRequest, index));
    }

    private HttpResponse processSearchRequest(final AggregatedHttpRequest aggregatedHttpRequest, String index) throws Exception {
        requestsReceivedCounterSearch.increment();
        payloadSizeSummarySearch.record(aggregatedHttpRequest.content().length());
        JacksonEvent event = JacksonEvent.builder().withEventType(EventType.DOCUMENT.toString()).withData(new String(aggregatedHttpRequest.content().toInputStream().readAllBytes(), StandardCharsets.UTF_8)).build();
        event.getMetadata().setAttribute(MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE_INDEX, index);
        List<Record<Event>> records = new ArrayList<>();
        records.add(new Record<>(event));
        Map<String, Object> sinkResponses = this.source.getPipeline().executeRequestFromSourceInline(records, true);

        if (sinkResponses.isEmpty()) {
            throw new InvalidObjectException("Internal Error");
        }
        SearchResponse sinkResponse = (SearchResponse) sinkResponses.get("OpenSearchSink");
        StringWriter jsonObjectWriter = new StringWriter();
        JsonGenerator generator = factory.createGenerator(jsonObjectWriter);
        generator.setCodec(new ObjectMapper());
        sinkResponse.serialize(new JacksonJsonpGenerator(generator), new JacksonJsonpMapper(objectMapper));
        generator.flush();
        String response = jsonObjectWriter.toString();
        successRequestsCounterSearch.increment();
        return HttpResponse.of(response);
    }

    private HttpResponse processBulkRequest(final AggregatedHttpRequest aggregatedHttpRequest, BulkAPIRequestParams bulkAPIRequestParams) throws Exception {
        final HttpData content = aggregatedHttpRequest.content();
        List<Map<String, Object>> jsonList;

        try {
            jsonList = jsonCodec.parse(content);
        } catch (IOException e) {
            LOG.error("Failed to parse the request of size {} due to: {}", content.length(), e.getMessage());
            throw new IOException("Bad request data format. Needs to be json array.", e.getCause());
        }
        List<Record<Event>> records = generateEventsFromInput(jsonList, bulkAPIRequestParams);
//        if (shouldExecuteAsync(jsonList)) {
//            return handleBulkRequestAsync(aggregatedHttpRequest, records);
//        }
        return handleBulkRequestSync(aggregatedHttpRequest, records);
    }

    private HttpResponse handleBulkRequestAsync(final AggregatedHttpRequest aggregatedHttpRequest, List<Record<Event>> records) throws Exception {
        final HttpData content = aggregatedHttpRequest.content();

        try {
            if (buffer.isByteBuffer()) {
                // jsonList is ignored in this path but parse() was done to make
                // sure that the data is in the expected json format
                buffer.writeBytes(content.array(), null, bufferWriteTimeoutInMillis);
            } else {
                buffer.writeAll(records, bufferWriteTimeoutInMillis);
            }
        } catch (Exception e) {
            LOG.error("Failed to write the request of size {} due to: {}", content.length(), e.getMessage());
            throw e;
        }
        successRequestsCounterBulk.increment();
        return HttpResponse.of(HttpStatus.OK);
    }

    private HttpResponse handleBulkRequestSync(final AggregatedHttpRequest aggregatedHttpRequest, List<Record<Event>> records) throws Exception {
        String response = "";
        HttpData content = null;
        try {
                content = aggregatedHttpRequest.content();
                Map<String, Object> sinkResponses = this.source.getPipeline().executeRequestFromSourceInline(records, false);
                if (sinkResponses.isEmpty()) {
                    throw new InvalidObjectException("Internal Error");
                }
                BulkResponse sinkResponse = (BulkResponse) sinkResponses.get("OpenSearchSink");
                StringWriter jsonObjectWriter = new StringWriter();
                JsonGenerator generator = factory.createGenerator(jsonObjectWriter);
                generator.setCodec(new ObjectMapper());
                sinkResponse.serialize(new JacksonJsonpGenerator(generator), new JacksonJsonpMapper(objectMapper));
                generator.flush();
                response = jsonObjectWriter.toString();
        } catch (Exception e) {
            LOG.error("Failed to write the request of size {} due to: {}", content.length(), e.getMessage());
            throw e;
        }
        successRequestsCounterBulk.increment();
        return HttpResponse.of(response);
    }

    private boolean shouldExecuteAsync(List<Map<String, Object>> jsonList) throws JsonProcessingException {
        int idx = 0;
        for (; idx<jsonList.size(); idx++) {
            Map<String, Object> jsonEntry = jsonList.get(idx);
            BulkActionRequest request = new BulkActionRequest(jsonEntry);
            boolean isValidBulkAction = Arrays.stream(OpenSearchBulkActions.values())
                    .anyMatch(bulkAction -> bulkAction == OpenSearchBulkActions.fromOptionValue(request.getAction()));
            if (isValidBulkAction) {
                if (!request.getAction().equals(OpenSearchBulkActions.INDEX.toString())) {
                    return false;
                }
            }
        }
        return true;
    }

    private List<Record<Event>> generateEventsFromInput(List<Map<String, Object>> jsonList, BulkAPIRequestParams bulkAPIRequestParams) throws JsonProcessingException {
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
                event.getMetadata().setAttribute(MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE_INDEX,
                        request.getIndex().isBlank() || request.getIndex().isEmpty() ? bulkAPIRequestParams.getIndex() : request.getIndex());
                String docId = request.getId();
                if (!StringUtils.isBlank(docId) && !StringUtils.isEmpty(docId)) {
                    event.getMetadata().setAttribute(MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE_ID, request.getId());
                }
                if (!StringUtils.isBlank(bulkAPIRequestParams.getPipeline()) && !StringUtils.isEmpty(bulkAPIRequestParams.getPipeline())) {
                    event.getMetadata().setAttribute(MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE_PIPELINE, bulkAPIRequestParams.getPipeline());
                }
                if (!StringUtils.isBlank(bulkAPIRequestParams.getRouting()) && !StringUtils.isEmpty(bulkAPIRequestParams.getRouting())) {
                    event.getMetadata().setAttribute(MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE_ROUTING, bulkAPIRequestParams.getRouting());
                }
                if (!StringUtils.isBlank(bulkAPIRequestParams.getRefresh()) && !StringUtils.isEmpty(bulkAPIRequestParams.getRefresh())) {
                    event.getMetadata().setAttribute(MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE_REFRESH, bulkAPIRequestParams.getRefresh());
                }

                // Skip processing next line
                if (!isDeleteAction) idx++;
                records.add(new Record<>(event));
            }
        }
        return records;
    }
}
