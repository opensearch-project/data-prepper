/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.processor.otelmetrics;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.processor.AbstractProcessor;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.processor.otelmetrics.model.RawGaugeBuilder;
import com.amazon.dataprepper.plugins.processor.otelmetrics.model.RawSum;
import com.amazon.dataprepper.plugins.processor.otelmetrics.model.RawSumBuilder;
import com.amazon.dataprepper.plugins.processor.otelmetrics.model.RawHistogram;
import com.amazon.dataprepper.plugins.processor.otelmetrics.model.RawHistogramBuilder;
import com.amazon.dataprepper.plugins.processor.otelmetrics.model.RawSummary;
import com.amazon.dataprepper.plugins.processor.otelmetrics.model.RawSummaryBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@DataPrepperPlugin(name = "otel_metrics_raw_processor", pluginType = Processor.class)
public class OTelMetricsStringProcessor extends AbstractProcessor<Record<ExportMetricsServiceRequest>, Record<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(OTelMetricsStringProcessor.class);

    public OTelMetricsStringProcessor(PluginSetting pluginSetting) {
        super(pluginSetting);
    }

    @Override
    public Collection<Record<String>> doExecute(Collection<Record<ExportMetricsServiceRequest>> records) {
        List<Record<String>> recs = new ArrayList<>();
        for (Record<ExportMetricsServiceRequest> ets : records) {
            for (ResourceMetrics rs : ets.getData().getResourceMetricsList()) {
                final Map<String, Object> resourceAttributes = OTelMetricsProtoHelper.getResourceAttributes(rs.getResource());
                final String serviceName = OTelMetricsProtoHelper.getServiceName(rs.getResource()).orElse(null);

                for (InstrumentationLibraryMetrics is : rs.getInstrumentationLibraryMetricsList()) {
                    for (Metric metric : is.getMetricsList()) {
                        if (metric.hasSum()) {
                            recs.addAll(mapSum(metric, serviceName, is.getInstrumentationLibrary(), resourceAttributes));
                        } else if (metric.hasGauge()) {
                            recs.addAll(mapGauge(metric, serviceName, is.getInstrumentationLibrary(), resourceAttributes));
                        } else if (metric.hasHistogram()) {
                            recs.addAll(mapHistogram(metric, serviceName, is.getInstrumentationLibrary(), resourceAttributes));
                        } else if (metric.hasSummary()) {
                            recs.addAll(mapSummary(metric, serviceName, is.getInstrumentationLibrary(), resourceAttributes));
                        }
                    }
                }
            }
        }
        return recs;
    }

    private List<Record<String>> mapSum(Metric metric, String serviceName, InstrumentationLibrary il, Map<String, Object> resourceAttributes) {
        List<NumberDataPoint> datapoints = metric.getSum().getDataPointsList();
        List<Record<String>> records = new ArrayList<>();
        for (NumberDataPoint dp : datapoints) {
            final RawSum rawSum = new RawSumBuilder()
                    .setFromSum(
                            metric,
                            il,
                            serviceName,
                            resourceAttributes,
                            dp)
                    .build();
            try {
                String json = rawSum.toJson();
                records.add(new Record<>(json));
            } catch (JsonProcessingException e) {
                LOG.error("Unable to process invalid Sum {}:", rawSum, e);
            }

        }
        return records;
    }

    private List<Record<String>> mapGauge(Metric metric, String serviceName, InstrumentationLibrary il, Map<String, Object> resourceAttributes) {
        return metric.getGauge().getDataPointsList().stream()
                .map(dp -> new RawGaugeBuilder()
                        .setFromGauge(
                                metric,
                                il,
                                serviceName,
                                resourceAttributes,
                                dp)
                        .build())
                .map(g -> {
                    Optional<Record<String>> rec = Optional.empty();
                    try {
                        rec = Optional.of(new Record<>(g.toJson()));
                    } catch (JsonProcessingException e) {
                        LOG.error("Unable to process invalid Gauge {}:", g, e);
                    }
                    return rec;
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    private List<Record<String>> mapHistogram(Metric metric, String serviceName, InstrumentationLibrary il, Map<String, Object> resourceAttributes) {
        return metric.getHistogram().getDataPointsList().stream()
                .map(dp -> {
                    final RawHistogram rawHistogram = new RawHistogramBuilder()
                            .setFromHistogram(
                                    metric,
                                    il,
                                    serviceName,
                                    resourceAttributes,
                                    dp)
                            .build();
                    Optional<Record<String>> rec = Optional.empty();
                    try {
                        rec = Optional.of(new Record<>(rawHistogram.toJson()));
                    } catch (JsonProcessingException e) {
                        LOG.error("Unable to process invalid Histogram {}:", rawHistogram, e);
                    }
                    return rec;
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    private List<Record<String>> mapSummary(Metric metric, String serviceName, InstrumentationLibrary il, Map<String, Object> resourceAttributes) {
        return metric.getSummary().getDataPointsList().stream()
                .map(dp -> {
                    RawSummary rawSummary = new RawSummaryBuilder()
                            .setFromSummary(
                                    metric,
                                    il,
                                    serviceName,
                                    resourceAttributes,
                                    dp)
                            .build();
                    Optional<Record<String>> rec = Optional.empty();
                    try {
                        rec = Optional.of(new Record<>(rawSummary.toJson()));
                    } catch (JsonProcessingException e) {
                        LOG.error("Unable to process invalid Summary {}:", rawSummary, e);
                    }

                    return rec;
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
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
