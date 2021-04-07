package com.amazon.dataprepper.plugins.prepper.oteltracegroup;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.AbstractPrepper;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

@DataPrepperPlugin(name = "otel_trace_group_prepper", type = PluginType.PREPPER)
public class OTelTraceGroupPrepper extends AbstractPrepper<Record<String>, Record<String>> {

    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceGroupPrepper.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};

    private final OTelTraceGroupPrepperConfig otelTraceGroupPrepperConfig;
    private final RestHighLevelClient restHighLevelClient;
    // TODO: add metrics

    public OTelTraceGroupPrepper(final PluginSetting pluginSetting) {
        super(pluginSetting);
        otelTraceGroupPrepperConfig = OTelTraceGroupPrepperConfig.buildConfig(pluginSetting);
        restHighLevelClient = otelTraceGroupPrepperConfig.getEsConnectionConfig().createClient();
    }

    @Override
    public Collection<Record<String>> doExecute(final Collection<Record<String>> rawSpanStringRecords) {
        final List<Record<String>> recordsOut = new LinkedList<>();
        final Map<Record<String>, Map<String, Object>> recordMissingTraceGroupToRawSpanMap = new HashMap<>();
        final Set<String> traceIdsToLookUp = new HashSet<>();
        for (Record<String> record: rawSpanStringRecords) {
            try {
                final Map<String, Object> rawSpanMap = OBJECT_MAPPER.readValue(record.getData(), MAP_TYPE_REFERENCE);
                final String traceGroupName = (String) rawSpanMap.get(TraceGroupWrapper.TRACE_GROUP_NAME_FIELD);
                final String traceId = (String) rawSpanMap.get(OTelTraceGroupPrepperConfig.TRACE_ID_FIELD);
                if (Strings.isNullOrEmpty(traceGroupName)) {
                    traceIdsToLookUp.add(traceId);
                    recordMissingTraceGroupToRawSpanMap.put(record, rawSpanMap);
                } else {
                    recordsOut.add(record);
                }
            } catch (JsonProcessingException e) {
                LOG.error("Failed to parse the record: [{}]", record.getData());
            }
        }

        final Map<String, TraceGroupWrapper> traceIdToTraceGroup = searchTraceGroupByTraceIds(traceIdsToLookUp);
        for (final Map.Entry<Record<String>, Map<String, Object>> entry: recordMissingTraceGroupToRawSpanMap.entrySet()) {
            final Record<String> record = entry.getKey();
            final Map<String, Object> rawSpanMap = entry.getValue();
            final String traceId = (String) rawSpanMap.get(OTelTraceGroupPrepperConfig.TRACE_ID_FIELD);
            final TraceGroupWrapper traceGroup = traceIdToTraceGroup.get(traceId);
            if (Objects.nonNull(traceGroup)) {
                rawSpanMap.put(TraceGroupWrapper.TRACE_GROUP_NAME_FIELD, traceGroup.getName());
                rawSpanMap.put(TraceGroupWrapper.TRACE_GROUP_END_TIME_FIELD, traceGroup.getEndTime());
                rawSpanMap.put(TraceGroupWrapper.TRACE_GROUP_DURATION_IN_NANOS_FIELD, traceGroup.getDurationInNanos());
                rawSpanMap.put(TraceGroupWrapper.TRACE_GROUP_STATUS_CODE_FIELD, traceGroup.getStatusCode());
                try {
                    final String newData = OBJECT_MAPPER.writeValueAsString(rawSpanMap);
                    recordsOut.add(new Record<>(newData, record.getMetadata()));
                } catch (JsonProcessingException e) {
                    recordsOut.add(record);
                    LOG.error("Failed to process the raw span: [{}]", record.getData(), e);
                }
            } else {
                recordsOut.add(record);
                final String spanId = (String) rawSpanMap.get(OTelTraceGroupPrepperConfig.SPAN_ID_FIELD);
                LOG.info("Failed to find traceGroup for spanId: {} due to traceGroup missing for traceId: {}", spanId, traceId);
            }
        }

        return recordsOut;
    }

    private Map<String, TraceGroupWrapper> searchTraceGroupByTraceIds(final Collection<String> traceIds) {
        final Map<String, TraceGroupWrapper> traceIdToTraceGroup = new HashMap<>();
        final SearchRequest searchRequest = createSearchRequest(traceIds);

        try {
            final SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            final SearchHit[] searchHits = searchResponse.getHits().getHits();
            Arrays.asList(searchHits).forEach(searchHit -> {
                Map.Entry<String, TraceGroupWrapper> entry = fromSearchHitToMapEntry(searchHit);
                if (Objects.nonNull(entry)) {
                    traceIdToTraceGroup.put(entry.getKey(), entry.getValue());
                }
            });
        } catch (Exception e) {
            // TODO: retry for status code 429 of ElasticsearchException?
            LOG.error("Search request for traceGroup failed for traceIds: {} due to {}", traceIds, e.getMessage());
        }

        return traceIdToTraceGroup;
    }

    private SearchRequest createSearchRequest(final Collection<String> traceIds) {
        final SearchRequest searchRequest = new SearchRequest(OTelTraceGroupPrepperConfig.RAW_INDEX_ALIAS);
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termsQuery(OTelTraceGroupPrepperConfig.TRACE_ID_FIELD, traceIds))
                        .must(QueryBuilders.termQuery(OTelTraceGroupPrepperConfig.PARENT_SPAN_ID_FIELD, ""))
        );
        searchSourceBuilder.docValueField(OTelTraceGroupPrepperConfig.TRACE_ID_FIELD);
        searchSourceBuilder.docValueField(TraceGroupWrapper.TRACE_GROUP_NAME_FIELD);
        searchSourceBuilder.docValueField(TraceGroupWrapper.TRACE_GROUP_END_TIME_FIELD);
        searchSourceBuilder.docValueField(TraceGroupWrapper.TRACE_GROUP_DURATION_IN_NANOS_FIELD);
        searchSourceBuilder.docValueField(TraceGroupWrapper.TRACE_GROUP_STATUS_CODE_FIELD);
        searchSourceBuilder.fetchSource(false);
        searchRequest.source(searchSourceBuilder);

        return searchRequest;
    }

    private Map.Entry<String, TraceGroupWrapper> fromSearchHitToMapEntry(final SearchHit searchHit) {
        final DocumentField traceIdDocField = searchHit.field(OTelTraceGroupPrepperConfig.TRACE_ID_FIELD);
        final DocumentField traceGroupNameDocField = searchHit.field(TraceGroupWrapper.TRACE_GROUP_NAME_FIELD);
        final DocumentField traceGroupEndTimeDocField = searchHit.field(TraceGroupWrapper.TRACE_GROUP_END_TIME_FIELD);
        final DocumentField traceGroupDurationInNanosDocField = searchHit.field(TraceGroupWrapper.TRACE_GROUP_DURATION_IN_NANOS_FIELD);
        final DocumentField traceGroupStatusCodeDocField = searchHit.field(TraceGroupWrapper.TRACE_GROUP_STATUS_CODE_FIELD);
        if (Stream.of(traceIdDocField, traceGroupNameDocField, traceGroupEndTimeDocField, traceGroupDurationInNanosDocField,
                traceGroupStatusCodeDocField).allMatch(Objects::nonNull)) {
            final String traceId = traceIdDocField.getValue();
            final String traceGroupName = traceGroupNameDocField.getValue();
            final String traceGroupEndTime = traceGroupEndTimeDocField.getValue();
            final Number traceGroupDurationInNanos = traceGroupDurationInNanosDocField.getValue();
            final Number traceGroupStatusCode = traceGroupStatusCodeDocField.getValue();
            return new AbstractMap.SimpleEntry<>(traceId, new TraceGroupWrapper(traceGroupName, traceGroupEndTime,
                    traceGroupDurationInNanos.longValue(), traceGroupStatusCode.intValue()));
        }
        return null;
    }

    @Override
    public void shutdown() {
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
