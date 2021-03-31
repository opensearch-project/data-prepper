package com.amazon.dataprepper.plugins.prepper.oteltracegroup;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.AbstractPrepper;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
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
        final List<Record<String>> recordsMissingTraceGroup = new ArrayList<>();
        final List<Map<String, Object>> rawSpanMapsMissingTraceGroup = new ArrayList<>();
        final Set<String> traceIdsToLookUp = new HashSet<>();
        for (Record<String> record: rawSpanStringRecords) {
            try {
                final Map<String, Object> rawSpanMap = OBJECT_MAPPER.readValue(record.getData(), MAP_TYPE_REFERENCE);
                final String traceGroup = (String) rawSpanMap.get(OTelTraceGroupPrepperConfig.TRACE_GROUP_FIELD);
                final String traceId = (String) rawSpanMap.get(OTelTraceGroupPrepperConfig.TRACE_ID_FIELD);
                if (traceGroup == null || traceGroup.equals("")) {
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

        final Map<String, String> traceIdToTraceGroup = searchTraceGroupByTraceIds(traceIdsToLookUp);
        for (int i = 0; i < recordsMissingTraceGroup.size(); i++) {
            final Map<String, Object> rawSpanMap = rawSpanMapsMissingTraceGroup.get(i);
            final Record<String> record = recordsMissingTraceGroup.get(i);
            final String traceId = (String) rawSpanMap.get(OTelTraceGroupPrepperConfig.TRACE_ID_FIELD);
            final String spanId = (String) rawSpanMap.get(OTelTraceGroupPrepperConfig.SPAN_ID_FIELD);
            final String traceGroup = traceIdToTraceGroup.get(traceId);
            if (traceGroup != null && !traceGroup.isEmpty()) {
                rawSpanMap.put(OTelTraceGroupPrepperConfig.TRACE_GROUP_FIELD, traceGroup);
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

    private Map<String, String> searchTraceGroupByTraceIds(final Collection<String> traceIds) {
        final Map<String, String> traceIdToTraceGroup = new HashMap<>();
        final SearchRequest searchRequest = new SearchRequest(OTelTraceGroupPrepperConfig.RAW_INDEX_ALIAS);
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termsQuery(OTelTraceGroupPrepperConfig.TRACE_ID_FIELD, traceIds))
                        .must(QueryBuilders.termQuery(OTelTraceGroupPrepperConfig.PARENT_SPAN_ID_FIELD, ""))
        );
        searchSourceBuilder.docValueField(OTelTraceGroupPrepperConfig.TRACE_ID_FIELD);
        searchSourceBuilder.docValueField(OTelTraceGroupPrepperConfig.TRACE_GROUP_FIELD);
        searchSourceBuilder.fetchSource(false);
        searchRequest.source(searchSourceBuilder);

        try {
            final SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            final SearchHit[] searchHits = searchResponse.getHits().getHits();
            Arrays.asList(searchHits).forEach(searchHit -> {
                final String traceId = searchHit.field(OTelTraceGroupPrepperConfig.TRACE_ID_FIELD).getValue();
                final String traceGroup = searchHit.field(OTelTraceGroupPrepperConfig.TRACE_GROUP_FIELD).getValue();
                traceIdToTraceGroup.put(traceId, traceGroup);
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
