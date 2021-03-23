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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@DataPrepperPlugin(name = "otel_trace_group_prepper", type = PluginType.PREPPER)
public class OtelTraceGroupPrepper extends AbstractPrepper<Record<String>, Record<String>> {

    private static final String RAW_INDEX_ALIAS = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexConstants.RAW);

    private static final Logger LOG = LoggerFactory.getLogger(OtelTraceGroupPrepper.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};

    private final OtelTraceGroupPrepperConfig otelTraceGroupPrepperConfig;
    private final RestHighLevelClient restHighLevelClient;

    public OtelTraceGroupPrepper(final PluginSetting pluginSetting) {
        super(pluginSetting);
        otelTraceGroupPrepperConfig = OtelTraceGroupPrepperConfig.buildConfig(pluginSetting);
        restHighLevelClient = otelTraceGroupPrepperConfig.getEsConnectionConfig().createClient();
    }

    @Override
    public Collection<Record<String>> doExecute(final Collection<Record<String>> records) {
        final List<Record<String>> RecordsOut = new LinkedList<>();
        for (Record<String> record: records) {
            Map<String, Object> rawSpanMap = null;
            try {
                rawSpanMap = OBJECT_MAPPER.readValue(record.getData(), MAP_TYPE_REFERENCE);
                final String traceGroup = (String) rawSpanMap.get("traceGroup");
                if (traceGroup == null || traceGroup.equals("")) {
                    boolean isSuccess = searchAndPopulateTraceGroup(rawSpanMap);
                    if (!isSuccess) {
                        // TODO: properly log the failure
                        String spanId = (String) rawSpanMap.get("spanId");
                        LOG.error("Failed to find traceGroup for spanId: {}", spanId);
                    }
                    String newData = OBJECT_MAPPER.writeValueAsString(rawSpanMap);
                    RecordsOut.add(new Record<>(newData, record.getMetadata()));
                } else {
                    RecordsOut.add(record);
                }
            } catch (JsonProcessingException e) {
                // TODO: log error properly
                e.printStackTrace();
            }
        }
        return null;
    }

    private boolean searchAndPopulateTraceGroup(Map<String, Object> rawSpanMap) {
        String traceId = (String) rawSpanMap.get("traceId");
        // TODO: query elasticsearch for traceId to traceGroup
        SearchRequest searchRequest = new SearchRequest(RAW_INDEX_ALIAS);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("traceId", traceId))
                        .must(QueryBuilders.matchQuery("parentSpanId", ""))
        );
        searchSourceBuilder.docValueField("traceId");
        searchSourceBuilder.docValueField("traceGroup");
        searchSourceBuilder.fetchSource(false);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            if (searchHits.length == 0) {
                // TODO: log failure to find traceGroup
                return false;
            }
            SearchHit searchHit = searchHits[0];
            String traceGroup = searchHit.field("traceGroup").getValue();
            if (traceGroup != null && !traceGroup.isEmpty()) {
                rawSpanMap.put("traceGroup", traceGroup);
                return true;
            } else {
                // TODO: log missing traceGroup
                return false;
            }
        } catch (Exception e) {
            // TODO: log error properly
            e.printStackTrace();
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
