package com.amazon.dataprepper.plugins.prepper.oteltracegroup;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.AbstractPrepper;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.record.RecordMetadata;
import com.amazon.dataprepper.plugins.prepper.oteltrace.model.TraceGroup;
import com.amazon.dataprepper.plugins.sink.elasticsearch.IndexConstants;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@DataPrepperPlugin(name = "otel_trace_group_prepper", type = PluginType.PREPPER)
public class OtelTraceGroupPrepper extends AbstractPrepper<Record<String>, Record<String>> {

    private static final Logger LOG = LoggerFactory.getLogger(OtelTraceGroupPrepper.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};

    private final OtelTraceGroupPrepperConfig otelTraceGroupPrepperConfig;
    private final RestHighLevelClient restHighLevelClient;
    // TODO: add metrics

    public OtelTraceGroupPrepper(final PluginSetting pluginSetting) {
        super(pluginSetting);
        otelTraceGroupPrepperConfig = OtelTraceGroupPrepperConfig.buildConfig(pluginSetting);
        restHighLevelClient = otelTraceGroupPrepperConfig.getEsConnectionConfig().createClient();
    }

    @Override
    public Collection<Record<String>> doExecute(final Collection<Record<String>> records) {
        final List<Record<String>> recordsOut = new LinkedList<>();
        final List<Record<String>> recordsMissingTraceGroup = new ArrayList<>();
        final List<Map<String, Object>> rawSpanMapsMissingTraceGroup = new ArrayList<>();
        final Set<String> traceIdsToLookUp = new HashSet<>();
        for (Record<String> record: records) {
            try {
                final Map<String, Object> rawSpanMap = OBJECT_MAPPER.readValue(record.getData(), MAP_TYPE_REFERENCE);
                final String traceGroupName = (String) rawSpanMap.get(OtelTraceGroupPrepperConfig.TRACE_GROUP_NAME_FIELD);
                final String traceId = (String) rawSpanMap.get(OtelTraceGroupPrepperConfig.TRACE_ID_FIELD);
                if (traceGroupName == null || traceGroupName.equals("")) {
                    traceIdsToLookUp.add(traceId);
                    recordsMissingTraceGroup.add(record);
                    rawSpanMapsMissingTraceGroup.add(rawSpanMap);
                } else {
                    recordsOut.add(record);
                }
            } catch (JsonProcessingException e) {
                recordsOut.add(record);
                LOG.error("Failed to parse the record: [{}]", record.getData());
            }
        }

        final Map<String, TraceGroup> traceIdToTraceGroup = searchTraceGroupByTraceIds(traceIdsToLookUp);
        for (int i = 0; i < recordsMissingTraceGroup.size(); i++) {
            final Map<String, Object> rawSpanMap = rawSpanMapsMissingTraceGroup.get(i);
            final Record<String> record = recordsMissingTraceGroup.get(i);
            final String traceId = (String) rawSpanMap.get(OtelTraceGroupPrepperConfig.TRACE_ID_FIELD);
            final String spanId = (String) rawSpanMap.get(OtelTraceGroupPrepperConfig.SPAN_ID_FIELD);
            final TraceGroup traceGroup = traceIdToTraceGroup.get(traceId);
            if (traceGroup != null) {
                rawSpanMap.put(OtelTraceGroupPrepperConfig.TRACE_GROUP_NAME_FIELD, traceGroup.getName());
                rawSpanMap.put(OtelTraceGroupPrepperConfig.TRACE_GROUP_END_TIME_FIELD, traceGroup.getEndTime());
                rawSpanMap.put(OtelTraceGroupPrepperConfig.TRACE_GROUP_DURATION_IN_NANOS_FIELD, traceGroup.getDurationInNanos());
                rawSpanMap.put(OtelTraceGroupPrepperConfig.TRACE_GROUP_STATUS_CODE_FIELD, traceGroup.getStatusCode());
                try {
                    final String newData = OBJECT_MAPPER.writeValueAsString(rawSpanMap);
                    recordsOut.add(new Record<>(newData, record.getMetadata()));
                } catch (JsonProcessingException e) {
                    recordsOut.add(record);
                    LOG.error("Failed to process the raw span: [{}]", record.getData(), e);
                }
            } else {
                recordsOut.add(record);
                LOG.info("Failed to find traceGroup for spanId: {} due to traceGroup missing for traceId: {}", spanId, traceId);
            }
        }

        return recordsOut;
    }

    private Map<String, TraceGroup> searchTraceGroupByTraceIds(final Collection<String> traceIds) {
        final Map<String, TraceGroup> traceIdToTraceGroup = new HashMap<>();
        final SearchRequest searchRequest = new SearchRequest(OtelTraceGroupPrepperConfig.RAW_INDEX_ALIAS);
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termsQuery(OtelTraceGroupPrepperConfig.TRACE_ID_FIELD, traceIds))
                        .must(QueryBuilders.termQuery(OtelTraceGroupPrepperConfig.PARENT_SPAN_ID_FIELD, ""))
        );
        searchSourceBuilder.docValueField(OtelTraceGroupPrepperConfig.TRACE_ID_FIELD);
        searchSourceBuilder.docValueField(OtelTraceGroupPrepperConfig.TRACE_GROUP_NAME_FIELD);
        searchSourceBuilder.docValueField(OtelTraceGroupPrepperConfig.TRACE_GROUP_END_TIME_FIELD);
        searchSourceBuilder.docValueField(OtelTraceGroupPrepperConfig.TRACE_GROUP_DURATION_IN_NANOS_FIELD);
        searchSourceBuilder.docValueField(OtelTraceGroupPrepperConfig.TRACE_GROUP_STATUS_CODE_FIELD);
        searchSourceBuilder.fetchSource(false);
        searchRequest.source(searchSourceBuilder);

        try {
            final SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            final SearchHit[] searchHits = searchResponse.getHits().getHits();
            Arrays.asList(searchHits).forEach(searchHit -> {
                final String traceId = searchHit.field(OtelTraceGroupPrepperConfig.TRACE_ID_FIELD).getValue();
                final String traceGroupName = searchHit.field(OtelTraceGroupPrepperConfig.TRACE_GROUP_NAME_FIELD).getValue();
                final String traceGroupEndTime = searchHit.field(OtelTraceGroupPrepperConfig.TRACE_GROUP_END_TIME_FIELD).getValue();
                final Number traceGroupDurationInNanos = searchHit.field(OtelTraceGroupPrepperConfig.TRACE_GROUP_DURATION_IN_NANOS_FIELD).getValue();
                final Number traceGroupStatusCode = searchHit.field(OtelTraceGroupPrepperConfig.TRACE_GROUP_STATUS_CODE_FIELD).getValue();
                traceIdToTraceGroup.put(traceId,
                        new TraceGroup.TraceGroupBuilder()
                                .setName(traceGroupName)
                                .setEndTime(traceGroupEndTime)
                                .setDurationInNanos(traceGroupDurationInNanos.longValue())
                                .setStatusCode(traceGroupStatusCode.intValue())
                                .build()
                );
            });
        } catch (Exception e) {
            // TODO: retry for status code 429 of ElasticsearchException?
            LOG.error("Search request for traceGroup failed for traceIds: {} due to {}", traceIds, e.getMessage());
        }

        return traceIdToTraceGroup;
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
