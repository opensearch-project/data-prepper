/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.otelmetrics;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.metric.Gauge;
import com.amazon.dataprepper.model.metric.Histogram;
import com.amazon.dataprepper.model.metric.JacksonGauge;
import com.amazon.dataprepper.model.metric.JacksonHistogram;
import com.amazon.dataprepper.model.metric.JacksonSum;
import com.amazon.dataprepper.model.metric.JacksonSummary;
import com.amazon.dataprepper.model.metric.Metric;
import com.amazon.dataprepper.model.metric.Sum;
import com.amazon.dataprepper.model.metric.Summary;
import com.amazon.dataprepper.model.processor.AbstractProcessor;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@DataPrepperPlugin(name = "otel_metrics_raw_processor", pluginType = Processor.class)
public class OTelMetricsRawProcessor extends AbstractProcessor<Record<ExportMetricsServiceRequest>, Record<? extends Metric>> {

    public OTelMetricsRawProcessor(PluginSetting pluginSetting) {
        super(pluginSetting);
    }

    @Override
    public Collection<Record<? extends Metric>> doExecute(Collection<Record<ExportMetricsServiceRequest>> records) {
        Collection<Record<? extends Metric>> recordsOut = new ArrayList<>();

        for (Record<ExportMetricsServiceRequest> ets : records) {
            for (ResourceMetrics rs : ets.getData().getResourceMetricsList()) {
                final Map<String, Object> resourceAttributes = OTelMetricsProtoHelper.getResourceAttributes(rs.getResource());
                final String serviceName = OTelMetricsProtoHelper.getServiceName(rs.getResource()).orElse(null);

                for (InstrumentationLibraryMetrics is : rs.getInstrumentationLibraryMetricsList()) {
                    final Map<String, Object> ils = OTelMetricsProtoHelper.getInstrumentationLibraryAttributes(is.getInstrumentationLibrary());
                    for (io.opentelemetry.proto.metrics.v1.Metric metric : is.getMetricsList()) {
                        if (metric.hasGauge()) {
                            recordsOut.addAll(mapGauge(metric, serviceName,  ils, resourceAttributes));
                        } else
                        if (metric.hasSum()) {
                            recordsOut.addAll(mapSum(metric, serviceName, ils, resourceAttributes));
                        } else
                        if (metric.hasSummary()) {
                            recordsOut.addAll(mapSummary(metric, serviceName, ils, resourceAttributes));
                        } else
                        if (metric.hasHistogram()) {
                            recordsOut.addAll(mapHistogram(metric, serviceName, ils, resourceAttributes));
                        }
                    }
                }
            }
        }
        return recordsOut;
    }

    private List<Record<Gauge>> mapGauge(io.opentelemetry.proto.metrics.v1.Metric metric, String serviceName, final Map<String, Object> ils, final Map<String, Object> resourceAttributes) {
        return metric.getGauge().getDataPointsList().stream()
                .map(dp -> (Gauge) JacksonGauge.builder()
                        .withUnit(metric.getUnit())
                        .withName(metric.getName())
                        .withDescription(metric.getDescription())
                        .withStartTime(OTelMetricsProtoHelper.getStartTimeISO8601(dp))
                        .withTime(OTelMetricsProtoHelper.getTimeISO8601(dp))
                        .withServiceName(serviceName)
                        .withValue(OTelMetricsProtoHelper.getValueAsMaybeDouble(dp))
                        .withAttributes(OTelMetricsProtoHelper.mergeAllAttributes(
                                Arrays.asList(
                                        OTelMetricsProtoHelper.convertKeysOfDataPointAttributes(dp),
                                        resourceAttributes,
                                        ils
                                )
                        ))
                        .build())
                .map(Record::new)
                .collect(Collectors.toList());
    }

    private List<Record<Sum>> mapSum(io.opentelemetry.proto.metrics.v1.Metric metric, String serviceName, final Map<String, Object> ils, final Map<String, Object> resourceAttributes) {
        return metric.getSum().getDataPointsList().stream()
                .map(dp -> (Sum) JacksonSum.builder()
                        .withUnit(metric.getUnit())
                        .withName(metric.getName())
                        .withDescription(metric.getDescription())
                        .withStartTime(OTelMetricsProtoHelper.getStartTimeISO8601(dp))
                        .withTime(OTelMetricsProtoHelper.getTimeISO8601(dp))
                        .withServiceName(serviceName)
                        .withIsMonotonic(metric.getSum().getIsMonotonic())
                        .withValue(OTelMetricsProtoHelper.getValueAsMaybeDouble(dp))
                        .withAggregationTemporality(metric.getSum().getAggregationTemporality().toString())
                        .withAttributes(OTelMetricsProtoHelper.mergeAllAttributes(
                                Arrays.asList(
                                        OTelMetricsProtoHelper.convertKeysOfDataPointAttributes(dp),
                                        resourceAttributes,
                                        ils
                                )
                        ))
                        .build())
                .map(Record::new)
                .collect(Collectors.toList());
    }

    private List<Record<Summary>> mapSummary(io.opentelemetry.proto.metrics.v1.Metric metric, String serviceName, final Map<String, Object> ils, final Map<String, Object> resourceAttributes) {
        return metric.getSummary().getDataPointsList().stream()
                .map(dp -> (Summary) JacksonSummary.builder()
                        .withUnit(metric.getUnit())
                        .withName(metric.getName())
                        .withDescription(metric.getDescription())
                        .withStartTime(OTelMetricsProtoHelper.convertUnixNanosToISO8601(dp.getStartTimeUnixNano()))
                        .withTime(OTelMetricsProtoHelper.convertUnixNanosToISO8601(dp.getTimeUnixNano()))
                        .withServiceName(serviceName)
                        .withCount(dp.getCount())
                        .withSum(dp.getSum())
                        .withQuantiles(OTelMetricsProtoHelper.getQuantileValues(dp.getQuantileValuesList()))
                        .withQuantilesValueCount(dp.getQuantileValuesCount())
                        .withAttributes(OTelMetricsProtoHelper.mergeAllAttributes(
                                Arrays.asList(
                                        OTelMetricsProtoHelper.unpackKeyValueList(dp.getAttributesList()),
                                        resourceAttributes,
                                        ils
                                )
                        ))
                        .build())
                .map(Record::new)
                .collect(Collectors.toList());
    }

    private List<Record<Histogram>> mapHistogram(io.opentelemetry.proto.metrics.v1.Metric metric, String serviceName, final Map<String, Object> ils, final Map<String, Object> resourceAttributes) {
        return metric.getHistogram().getDataPointsList().stream()
                .map(dp -> (Histogram) JacksonHistogram.builder()
                        .withUnit(metric.getUnit())
                        .withName(metric.getName())
                        .withDescription(metric.getDescription())
                        .withStartTime(OTelMetricsProtoHelper.convertUnixNanosToISO8601(dp.getStartTimeUnixNano()))
                        .withTime(OTelMetricsProtoHelper.convertUnixNanosToISO8601(dp.getTimeUnixNano()))
                        .withServiceName(serviceName)
                        .withSum(dp.getSum())
                        .withBucketCount(dp.getBucketCountsCount())
                        .withExplicitBoundsCount(dp.getExplicitBoundsCount())
                        .withAggregationTemporality(metric.getHistogram().getAggregationTemporality().toString())
                        .withBuckets(OTelMetricsProtoHelper.createBuckets(dp.getBucketCountsList(), dp.getExplicitBoundsList()))
                        .withAttributes(OTelMetricsProtoHelper.mergeAllAttributes(
                                Arrays.asList(
                                        OTelMetricsProtoHelper.unpackKeyValueList(dp.getAttributesList()),
                                        resourceAttributes,
                                        ils
                                )
                        ))
                        .build())
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
