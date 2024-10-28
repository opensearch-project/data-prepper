/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.model.trace.TraceGroupFields;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.document.DocumentField;
import org.opensearch.dataprepper.plugins.processor.oteltracegroup.model.TraceGroup;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;

@DataPrepperPlugin(name = "otel_trace_group", pluginType = Processor.class,
        pluginConfigurationType = OTelTraceGroupProcessorConfig.class)
public class OTelTraceGroupProcessor extends AbstractProcessor<Record<Span>, Record<Span>> {

    public static final String RECORDS_IN_MISSING_TRACE_GROUP = "recordsInMissingTraceGroup";
    public static final String RECORDS_OUT_FIXED_TRACE_GROUP = "recordsOutFixedTraceGroup";
    public static final String RECORDS_OUT_MISSING_TRACE_GROUP = "recordsOutMissingTraceGroup";

    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceGroupProcessor.class);

    private final OTelTraceGroupProcessorConfig otelTraceGroupProcessorConfig;
    private final RestHighLevelClient restHighLevelClient;

    private final Counter recordsInMissingTraceGroupCounter;
    private final Counter recordsOutFixedTraceGroupCounter;
    private final Counter recordsOutMissingTraceGroupCounter;

    @DataPrepperPluginConstructor
    public OTelTraceGroupProcessor(final PluginMetrics pluginMetrics,
                                   final OTelTraceGroupProcessorConfig otelTraceGroupProcessorConfig,
                                   final AwsCredentialsSupplier awsCredentialsSupplier) {
        super(pluginMetrics);
        this.otelTraceGroupProcessorConfig = otelTraceGroupProcessorConfig;
        final OpenSearchClientFactory openSearchClientFactory = OpenSearchClientFactory.fromConnectionConfiguration(
                otelTraceGroupProcessorConfig.getEsConnectionConfig());
        restHighLevelClient = openSearchClientFactory.createRestHighLevelClient(awsCredentialsSupplier);

        recordsInMissingTraceGroupCounter = pluginMetrics.counter(RECORDS_IN_MISSING_TRACE_GROUP);
        recordsOutFixedTraceGroupCounter = pluginMetrics.counter(RECORDS_OUT_FIXED_TRACE_GROUP);
        recordsOutMissingTraceGroupCounter = pluginMetrics.counter(RECORDS_OUT_MISSING_TRACE_GROUP);
    }

    @Override
    public Collection<Record<Span>> doExecute(final Collection<Record<Span>> rawSpanRecords) {
        final List<Record<Span>> recordsOut = new LinkedList<>();
        final Set<Record<Span>> recordsMissingTraceGroupInfo = new HashSet<>();
        final Set<String> traceIdsToLookUp = new HashSet<>();
        for (Record<Span> record: rawSpanRecords) {
            final Span span = record.getData();
            final String traceGroup = span.getTraceGroup();
            final String traceId = span.getTraceId();
            if (Strings.isNullOrEmpty(traceGroup)) {
                traceIdsToLookUp.add(traceId);
                recordsMissingTraceGroupInfo.add(record);
                recordsInMissingTraceGroupCounter.increment();
            } else {
                recordsOut.add(record);
            }
        }

        final Map<String, TraceGroup> traceIdToTraceGroup = searchTraceGroupByTraceIds(traceIdsToLookUp);
        for (final Record<Span> record: recordsMissingTraceGroupInfo) {
            final Span span = record.getData();
            final String traceId = span.getTraceId();
            final TraceGroup traceGroup = traceIdToTraceGroup.get(traceId);
            if (traceGroup != null) {
                try {
                    fillInTraceGroupInfo(span, traceGroup);
                    recordsOut.add(record);
                    recordsOutFixedTraceGroupCounter.increment();
                } catch (Exception e) {
                    recordsOut.add(record);
                    recordsOutMissingTraceGroupCounter.increment();
                    LOG.error(EVENT, "Failed to process the span: [{}]", record.getData(), e);
                }
            } else {
                recordsOut.add(record);
                recordsOutMissingTraceGroupCounter.increment();
                final String spanId = span.getSpanId();
                LOG.warn("Failed to find traceGroup for spanId: {} due to traceGroup missing for traceId: {}", spanId, traceId);
            }
        }

        return recordsOut;
    }

    private void fillInTraceGroupInfo(final Span span, final TraceGroup traceGroup) {
        span.setTraceGroup(traceGroup.getTraceGroup());
        span.setTraceGroupFields(traceGroup.getTraceGroupFields());
    }

    private Map<String, TraceGroup> searchTraceGroupByTraceIds(final Collection<String> traceIds) {
        final Map<String, TraceGroup> traceIdToTraceGroup = new HashMap<>();
        final SearchRequest searchRequest = createSearchRequest(traceIds);

        try {
            final SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            final SearchHit[] searchHits = searchResponse.getHits().getHits();
            Arrays.asList(searchHits).forEach(searchHit -> {
                final Optional<Map.Entry<String, TraceGroup>> optionalStringTraceGroupEntry = fromSearchHitToMapEntry(searchHit);
                optionalStringTraceGroupEntry.ifPresent(entry -> traceIdToTraceGroup.put(entry.getKey(), entry.getValue()));
            });
        } catch (Exception e) {
            // TODO: retry for status code 429 of OpenSearchException?
            LOG.error("Search request for traceGroup failed for traceIds: {} due to {}", traceIds, e.getMessage());
        }

        return traceIdToTraceGroup;
    }

    private SearchRequest createSearchRequest(final Collection<String> traceIds) {
        final SearchRequest searchRequest = new SearchRequest(OTelTraceGroupProcessorConfig.RAW_INDEX_ALIAS);
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termsQuery(OTelTraceGroupProcessorConfig.TRACE_ID_FIELD, traceIds))
                        .must(QueryBuilders.termQuery(OTelTraceGroupProcessorConfig.PARENT_SPAN_ID_FIELD, ""))
        );
        searchSourceBuilder.docValueField(OTelTraceGroupProcessorConfig.TRACE_ID_FIELD);
        searchSourceBuilder.docValueField(TraceGroup.TRACE_GROUP_NAME_FIELD);
        searchSourceBuilder.docValueField(TraceGroup.TRACE_GROUP_END_TIME_FIELD, OTelTraceGroupProcessorConfig.STRICT_DATE_TIME);
        searchSourceBuilder.docValueField(TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD);
        searchSourceBuilder.docValueField(TraceGroup.TRACE_GROUP_STATUS_CODE_FIELD);
        searchSourceBuilder.fetchSource(false);
        searchRequest.source(searchSourceBuilder);

        return searchRequest;
    }

    private Optional<Map.Entry<String, TraceGroup>> fromSearchHitToMapEntry(final SearchHit searchHit) {
        final DocumentField traceIdDocField = searchHit.field(OTelTraceGroupProcessorConfig.TRACE_ID_FIELD);
        final DocumentField traceGroupNameDocField = searchHit.field(TraceGroup.TRACE_GROUP_NAME_FIELD);
        final DocumentField traceGroupEndTimeDocField = searchHit.field(TraceGroup.TRACE_GROUP_END_TIME_FIELD);
        final DocumentField traceGroupDurationInNanosDocField = searchHit.field(TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD);
        final DocumentField traceGroupStatusCodeDocField = searchHit.field(TraceGroup.TRACE_GROUP_STATUS_CODE_FIELD);
        if (Stream.of(traceIdDocField, traceGroupNameDocField, traceGroupEndTimeDocField, traceGroupDurationInNanosDocField,
                traceGroupStatusCodeDocField).allMatch(Objects::nonNull)) {
            final String traceId = traceIdDocField.getValue();
            final String traceGroupName = traceGroupNameDocField.getValue();
            final String traceGroupEndTime = normalizeDateTime(traceGroupEndTimeDocField.getValue());
            final Number traceGroupDurationInNanos = traceGroupDurationInNanosDocField.getValue();
            final Number traceGroupStatusCode = traceGroupStatusCodeDocField.getValue();
            final TraceGroupFields traceGroupFields = DefaultTraceGroupFields.builder()
                    .withEndTime(traceGroupEndTime)
                    .withDurationInNanos(traceGroupDurationInNanos.longValue())
                    .withStatusCode(traceGroupStatusCode.intValue())
                    .build();
            final TraceGroup traceGroup = new TraceGroup.TraceGroupBuilder()
                    .setTraceGroup(traceGroupName)
                    .setTraceGroupFields(traceGroupFields)
                    .build();
            return Optional.of(new AbstractMap.SimpleEntry<>(traceId, traceGroup));
        }
        return Optional.empty();
    }

    /**
     * Restores trailing zeros for thousand, e.g. 2020-08-20T05:40:46.0895568Z -> 2020-08-20T05:40:46.089556800Z
     */
    private String normalizeDateTime(String dateTimeString) {
        return Instant.parse(dateTimeString).toString();
    }

    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
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
