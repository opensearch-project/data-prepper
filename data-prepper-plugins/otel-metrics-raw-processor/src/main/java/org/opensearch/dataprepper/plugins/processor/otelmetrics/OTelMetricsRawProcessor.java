/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.otelmetrics;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.metric.JacksonExponentialHistogram;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonSummary;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import io.micrometer.core.instrument.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@DataPrepperPlugin(name = "otel_metrics", deprecatedName = "otel_metrics_raw_processor", pluginType = Processor.class, pluginConfigurationType = OtelMetricsRawProcessorConfig.class)
public class OTelMetricsRawProcessor extends AbstractProcessor<Record<ExportMetricsServiceRequest>, Record<? extends Metric>> {

    private static final Logger LOG = LoggerFactory.getLogger(OTelMetricsRawProcessor.class);
    public static final String RECORDS_DROPPED_METRICS_RAW = "recordsDroppedMetricsRaw";

    private final OtelMetricsRawProcessorConfig otelMetricsRawProcessorConfig;
    private final boolean flattenAttributesFlag;

    private final Counter recordsDroppedMetricsRawCounter;

    @DataPrepperPluginConstructor
    public OTelMetricsRawProcessor(PluginSetting pluginSetting, final OtelMetricsRawProcessorConfig otelMetricsRawProcessorConfig) {
        super(pluginSetting);
        this.otelMetricsRawProcessorConfig = otelMetricsRawProcessorConfig;
        recordsDroppedMetricsRawCounter = pluginMetrics.counter(RECORDS_DROPPED_METRICS_RAW);
        this.flattenAttributesFlag = otelMetricsRawProcessorConfig.getFlattenAttributesFlag();
    }

    @Override
    public Collection<Record<? extends Metric>> doExecute(Collection<Record<ExportMetricsServiceRequest>> records) {
        Collection<Record<? extends Metric>> recordsOut = new ArrayList<>();
        for (Record<ExportMetricsServiceRequest> ets : records) {
            for (ResourceMetrics rs : ets.getData().getResourceMetricsList()) {
                final String schemaUrl = rs.getSchemaUrl();
                final Map<String, Object> resourceAttributes = OTelProtoCodec.getResourceAttributes(rs.getResource());
                final String serviceName = OTelProtoCodec.getServiceName(rs.getResource()).orElse(null);

                for (InstrumentationLibraryMetrics is : rs.getInstrumentationLibraryMetricsList()) {
                    final Map<String, Object> ils = OTelProtoCodec.getInstrumentationLibraryAttributes(is.getInstrumentationLibrary());
                    recordsOut.addAll(processMetricsList(is.getMetricsList(), serviceName, ils, resourceAttributes, schemaUrl));
                }

                for (ScopeMetrics sm : rs.getScopeMetricsList()) {
                    final Map<String, Object> ils = OTelProtoCodec.getInstrumentationScopeAttributes(sm.getScope());
                    recordsOut.addAll(processMetricsList(sm.getMetricsList(), serviceName, ils, resourceAttributes, schemaUrl));
                }
            }
        }
        return recordsOut;
    }

    private List<? extends Record<? extends Metric>> processMetricsList(final List<io.opentelemetry.proto.metrics.v1.Metric> metricsList,
                                                                        final String serviceName,
                                                                        final Map<String, Object> ils,
                                                                        final Map<String, Object> resourceAttributes,
                                                                        final String schemaUrl) {
        List<Record<? extends Metric>> recordsOut = new ArrayList<>();
        for (io.opentelemetry.proto.metrics.v1.Metric metric : metricsList) {
            try {
                if (metric.hasGauge()) {
                    recordsOut.addAll(mapGauge(metric, serviceName, ils, resourceAttributes, schemaUrl));
                } else if (metric.hasSum()) {
                    recordsOut.addAll(mapSum(metric, serviceName, ils, resourceAttributes, schemaUrl));
                } else if (metric.hasSummary()) {
                    recordsOut.addAll(mapSummary(metric, serviceName, ils, resourceAttributes, schemaUrl));
                } else if (metric.hasHistogram()) {
                    recordsOut.addAll(mapHistogram(metric, serviceName, ils, resourceAttributes, schemaUrl));
                } else if (metric.hasExponentialHistogram()) {
                    recordsOut.addAll(mapExponentialHistogram(metric, serviceName, ils, resourceAttributes, schemaUrl));
                }
            } catch (Exception e) {
                LOG.warn("Error while processing metrics", e);
                recordsDroppedMetricsRawCounter.increment();
            }
        }
        return recordsOut;
    }

    private List<? extends Record<? extends Metric>> mapGauge(io.opentelemetry.proto.metrics.v1.Metric metric,
                                         String serviceName,
                                         final Map<String, Object> ils,
                                         final Map<String, Object> resourceAttributes,
                                         final String schemaUrl) {
        return metric.getGauge().getDataPointsList().stream()
                .map(dp -> JacksonGauge.builder()
                        .withUnit(metric.getUnit())
                        .withName(metric.getName())
                        .withDescription(metric.getDescription())
                        .withStartTime(OTelProtoCodec.getStartTimeISO8601(dp))
                        .withTime(OTelProtoCodec.getTimeISO8601(dp))
                        .withServiceName(serviceName)
                        .withValue(OTelProtoCodec.getValueAsDouble(dp))
                        .withAttributes(OTelProtoCodec.mergeAllAttributes(
                                Arrays.asList(
                                        OTelProtoCodec.convertKeysOfDataPointAttributes(dp),
                                        resourceAttributes,
                                        ils
                                )
                        ))
                        .withSchemaUrl(schemaUrl)
                        .withExemplars(OTelProtoCodec.convertExemplars(dp.getExemplarsList()))
                        .withFlags(dp.getFlags())
                        .build(flattenAttributesFlag))
                .map(Record::new)
                .collect(Collectors.toList());
    }

    private List<? extends Record<? extends Metric>> mapSum(final io.opentelemetry.proto.metrics.v1.Metric metric,
                                     final String serviceName,
                                     final Map<String, Object> ils,
                                     final Map<String, Object> resourceAttributes,
                                     final String schemaUrl) {
        return metric.getSum().getDataPointsList().stream()
                .map(dp -> JacksonSum.builder()
                        .withUnit(metric.getUnit())
                        .withName(metric.getName())
                        .withDescription(metric.getDescription())
                        .withStartTime(OTelProtoCodec.getStartTimeISO8601(dp))
                        .withTime(OTelProtoCodec.getTimeISO8601(dp))
                        .withServiceName(serviceName)
                        .withIsMonotonic(metric.getSum().getIsMonotonic())
                        .withValue(OTelProtoCodec.getValueAsDouble(dp))
                        .withAggregationTemporality(metric.getSum().getAggregationTemporality().toString())
                        .withAttributes(OTelProtoCodec.mergeAllAttributes(
                                Arrays.asList(
                                        OTelProtoCodec.convertKeysOfDataPointAttributes(dp),
                                        resourceAttributes,
                                        ils
                                )
                        ))
                        .withSchemaUrl(schemaUrl)
                        .withExemplars(OTelProtoCodec.convertExemplars(dp.getExemplarsList()))
                        .withFlags(dp.getFlags())
                        .build(flattenAttributesFlag))
                .map(Record::new)
                .collect(Collectors.toList());
    }

    private List<? extends Record<? extends Metric>> mapSummary(final io.opentelemetry.proto.metrics.v1.Metric metric,
                                             final String serviceName,
                                             final Map<String, Object> ils,
                                             final Map<String, Object> resourceAttributes,
                                             final String schemaUrl) {
        return metric.getSummary().getDataPointsList().stream()
                .map(dp -> JacksonSummary.builder()
                        .withUnit(metric.getUnit())
                        .withName(metric.getName())
                        .withDescription(metric.getDescription())
                        .withStartTime(OTelProtoCodec.convertUnixNanosToISO8601(dp.getStartTimeUnixNano()))
                        .withTime(OTelProtoCodec.convertUnixNanosToISO8601(dp.getTimeUnixNano()))
                        .withServiceName(serviceName)
                        .withCount(dp.getCount())
                        .withSum(dp.getSum())
                        .withQuantiles(OTelProtoCodec.getQuantileValues(dp.getQuantileValuesList()))
                        .withQuantilesValueCount(dp.getQuantileValuesCount())
                        .withAttributes(OTelProtoCodec.mergeAllAttributes(
                                Arrays.asList(
                                        OTelProtoCodec.unpackKeyValueList(dp.getAttributesList()),
                                        resourceAttributes,
                                        ils
                                )
                        ))
                        .withSchemaUrl(schemaUrl)
                        .withFlags(dp.getFlags())
                        .build(flattenAttributesFlag))
                .map(Record::new)
                .collect(Collectors.toList());
    }

    private List<? extends Record<? extends Metric>> mapHistogram(final io.opentelemetry.proto.metrics.v1.Metric metric,
                                                 final String serviceName,
                                                 final Map<String, Object> ils,
                                                 final Map<String, Object> resourceAttributes,
                                                 final String schemaUrl) {
        return metric.getHistogram().getDataPointsList().stream()
                .map(dp -> {
                    JacksonHistogram.Builder builder = JacksonHistogram.builder()
                            .withUnit(metric.getUnit())
                            .withName(metric.getName())
                            .withDescription(metric.getDescription())
                            .withStartTime(OTelProtoCodec.convertUnixNanosToISO8601(dp.getStartTimeUnixNano()))
                            .withTime(OTelProtoCodec.convertUnixNanosToISO8601(dp.getTimeUnixNano()))
                            .withServiceName(serviceName)
                            .withSum(dp.getSum())
                            .withCount(dp.getCount())
                            .withBucketCount(dp.getBucketCountsCount())
                            .withExplicitBoundsCount(dp.getExplicitBoundsCount())
                            .withAggregationTemporality(metric.getHistogram().getAggregationTemporality().toString())
                            .withBucketCountsList(dp.getBucketCountsList())
                            .withExplicitBoundsList(dp.getExplicitBoundsList())
                            .withAttributes(OTelProtoCodec.mergeAllAttributes(
                                    Arrays.asList(
                                            OTelProtoCodec.unpackKeyValueList(dp.getAttributesList()),
                                            resourceAttributes,
                                            ils
                                    )
                            ))
                            .withSchemaUrl(schemaUrl)
                            .withExemplars(OTelProtoCodec.convertExemplars(dp.getExemplarsList()))
                            .withFlags(dp.getFlags());
                    if (otelMetricsRawProcessorConfig.getCalculateHistogramBuckets()) {
                        builder.withBuckets(OTelProtoCodec.createBuckets(dp.getBucketCountsList(), dp.getExplicitBoundsList()));
                    }
                    JacksonHistogram jh = builder.build(flattenAttributesFlag);
                    return jh;

                })
                .map(Record::new)
                .collect(Collectors.toList());
    }

    private List<? extends Record<? extends Metric>> mapExponentialHistogram(io.opentelemetry.proto.metrics.v1.Metric metric, String serviceName, Map<String, Object> ils, Map<String, Object> resourceAttributes, String schemaUrl) {
        return metric.getExponentialHistogram().getDataPointsList().stream()
                .filter(dp -> {
                    if (otelMetricsRawProcessorConfig.getCalculateExponentialHistogramBuckets() &&
                            otelMetricsRawProcessorConfig.getExponentialHistogramMaxAllowedScale() < Math.abs(dp.getScale())){
                        LOG.error("Exponential histogram can not be processed since its scale of {} is bigger than the configured max of {}.", dp.getScale(), otelMetricsRawProcessorConfig.getExponentialHistogramMaxAllowedScale());
                        return false;
                    } else {
                        return true;
                    }
                })
                .map(dp -> {
                    JacksonExponentialHistogram.Builder builder = JacksonExponentialHistogram.builder()
                            .withUnit(metric.getUnit())
                            .withName(metric.getName())
                            .withDescription(metric.getDescription())
                            .withStartTime(OTelProtoCodec.convertUnixNanosToISO8601(dp.getStartTimeUnixNano()))
                            .withTime(OTelProtoCodec.convertUnixNanosToISO8601(dp.getTimeUnixNano()))
                            .withServiceName(serviceName)
                            .withSum(dp.getSum())
                            .withCount(dp.getCount())
                            .withZeroCount(dp.getZeroCount())
                            .withScale(dp.getScale())
                            .withPositive(dp.getPositive().getBucketCountsList())
                            .withPositiveOffset(dp.getPositive().getOffset())
                            .withNegative(dp.getNegative().getBucketCountsList())
                            .withNegativeOffset(dp.getNegative().getOffset())
                            .withAggregationTemporality(metric.getHistogram().getAggregationTemporality().toString())
                            .withAttributes(OTelProtoCodec.mergeAllAttributes(
                                    Arrays.asList(
                                            OTelProtoCodec.unpackKeyValueList(dp.getAttributesList()),
                                            resourceAttributes,
                                            ils
                                    )
                            ))
                            .withSchemaUrl(schemaUrl)
                            .withExemplars(OTelProtoCodec.convertExemplars(dp.getExemplarsList()))
                            .withFlags(dp.getFlags());

                    if (otelMetricsRawProcessorConfig.getCalculateExponentialHistogramBuckets()) {
                        builder.withPositiveBuckets(OTelProtoCodec.createExponentialBuckets(dp.getPositive(), dp.getScale()));
                        builder.withNegativeBuckets(OTelProtoCodec.createExponentialBuckets(dp.getNegative(), dp.getScale()));
                    }

                    return builder.build(flattenAttributesFlag);
                })
                .map(Record::new)
                .collect(Collectors.toList());
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
    }
}
