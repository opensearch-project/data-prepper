package com.amazon.dataprepper.plugins.prepper.oteltracegroup;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.AbstractPrepper;
import com.amazon.dataprepper.model.record.Record;
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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
        for (Record<String> record: records) {
            try {
                final Map<String, Object> rawSpanMap = OBJECT_MAPPER.readValue(record.getData(), MAP_TYPE_REFERENCE);
                final String traceGroup = (String) rawSpanMap.get("traceGroup");
                if (traceGroup == null || traceGroup.equals("")) {
                    final boolean isSuccess = searchAndPopulateTraceGroup(rawSpanMap);
                    if (isSuccess) {
                        final String newData = OBJECT_MAPPER.writeValueAsString(rawSpanMap);
                        recordsOut.add(new Record<>(newData, record.getMetadata()));
                    } else {
                        recordsOut.add(record);
                    }
                } else {
                    recordsOut.add(record);
                }
            } catch (JsonProcessingException e) {
                LOG.error("Failed to parse the record: [{}]", record.getData());
            }
        }
        return recordsOut;
    }

    private boolean searchAndPopulateTraceGroup(final Map<String, Object> rawSpanMap) {
        final String traceId = (String) rawSpanMap.get(OtelTraceGroupPrepperConfig.TRACE_ID_FIELD);
        final String spanId = (String) rawSpanMap.get(OtelTraceGroupPrepperConfig.SPAN_ID_FIELD);
        final SearchRequest searchRequest = new SearchRequest(OtelTraceGroupPrepperConfig.RAW_INDEX_ALIAS);
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(OtelTraceGroupPrepperConfig.TRACE_ID_FIELD, traceId))
                        .must(QueryBuilders.termQuery(OtelTraceGroupPrepperConfig.PARENT_SPAN_ID_FIELD, ""))
        );
        searchSourceBuilder.docValueField(OtelTraceGroupPrepperConfig.TRACE_GROUP_FIELD);
        searchSourceBuilder.fetchSource(false);
        searchRequest.source(searchSourceBuilder);
        try {
            final SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            final SearchHit[] searchHits = searchResponse.getHits().getHits();
            if (searchHits.length == 0) {
                LOG.info("Failed to populate traceGroup for spanId: {} due to root span not found for traceId: {}", spanId, traceId);
                return false;
            }
            final SearchHit searchHit = searchHits[0];
            final DocumentField traceGroupField = searchHit.field(OtelTraceGroupPrepperConfig.TRACE_GROUP_FIELD);
            if (traceGroupField == null) {
                LOG.info("Failed to populate traceGroup for spanId: {} due to traceGroup missing for traceId: {}", spanId, traceId);
                return false;
            }
            final String traceGroup = traceGroupField.getValue();
            if (traceGroup != null && !traceGroup.isEmpty()) {
                rawSpanMap.put(OtelTraceGroupPrepperConfig.TRACE_GROUP_FIELD, traceGroup);
                return true;
            } else {
                LOG.info("Failed to populate traceGroup for spanId: {} due to traceGroup missing for traceId: {}", spanId, traceId);
                return false;
            }
        } catch (Exception e) {
            LOG.error("Failed to populate traceGroup for spanId: {} due to {}", spanId, e.getMessage());
            return false;
        }
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
