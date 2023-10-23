/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.otelmetrics;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import static org.opensearch.dataprepper.model.metric.JacksonExponentialHistogram.POSITIVE_BUCKETS_KEY;
import static org.opensearch.dataprepper.model.metric.JacksonExponentialHistogram.NEGATIVE_BUCKETS_KEY;
import static org.opensearch.dataprepper.model.metric.JacksonHistogram.BUCKETS_KEY;
import org.opensearch.dataprepper.model.metric.Metric;
import static org.opensearch.dataprepper.model.metric.JacksonMetric.ATTRIBUTES_KEY;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import io.micrometer.core.instrument.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


@DataPrepperPlugin(name = "otel_metrics", deprecatedName = "otel_metrics_raw_processor", pluginType = Processor.class, pluginConfigurationType = OtelMetricsRawProcessorConfig.class)
public class OTelMetricsRawProcessor extends AbstractProcessor<Record<?>, Record<? extends Metric>> {

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

    private void modifyRecord(Record<? extends Metric> record,
                              boolean flattenAttributes,
                              boolean calcualteHistogramBuckets,
                              boolean calcualteExponentialHistogramBuckets) {
        Event event = (Event)record.getData();

        if (flattenAttributes) {
            Map<String, Object> attributes = event.get(ATTRIBUTES_KEY, Map.class);

            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                event.put(entry.getKey(), entry.getValue());
            }
            event.delete(ATTRIBUTES_KEY);
        }
        if (!calcualteHistogramBuckets && event.get(BUCKETS_KEY, List.class) != null) {
            event.delete(BUCKETS_KEY);
        }
        if (!calcualteExponentialHistogramBuckets) {
            if (event.get(POSITIVE_BUCKETS_KEY, List.class) != null) {
                event.delete(POSITIVE_BUCKETS_KEY);
            }
            if (event.get(NEGATIVE_BUCKETS_KEY, List.class) != null) {
                event.delete(NEGATIVE_BUCKETS_KEY);
            }
        }
    }

    @Override
    public Collection<Record<? extends Metric>> doExecute(Collection<Record<?>> records) {
        Collection<Record<? extends Metric>> recordsOut = new ArrayList<>();
        OTelProtoCodec.OTelProtoDecoder otelProtoDecoder = new OTelProtoCodec.OTelProtoDecoder();
        AtomicInteger droppedCounter = new AtomicInteger(0);

        for (Record<?> rec : records) {
            Record<? extends Metric> newRecord = (Record<? extends Metric>)rec;
            if ((rec.getData() instanceof Event)) {
                if (otelMetricsRawProcessorConfig.getFlattenAttributesFlag() ||
                    !otelMetricsRawProcessorConfig.getCalculateHistogramBuckets() ||
                    !otelMetricsRawProcessorConfig.getCalculateExponentialHistogramBuckets()) {
                    modifyRecord(newRecord, otelMetricsRawProcessorConfig.getFlattenAttributesFlag(), otelMetricsRawProcessorConfig.getCalculateHistogramBuckets(), otelMetricsRawProcessorConfig.getCalculateExponentialHistogramBuckets());
                }
            }
            recordsOut.add(newRecord);

            if (!(rec.getData() instanceof ExportMetricsServiceRequest)) {
                continue;
            }

            ExportMetricsServiceRequest request = ((Record<ExportMetricsServiceRequest>)rec).getData();
            recordsOut.addAll(otelProtoDecoder.parseExportMetricsServiceRequest(request, droppedCounter, otelMetricsRawProcessorConfig.getExponentialHistogramMaxAllowedScale(), otelMetricsRawProcessorConfig.getCalculateHistogramBuckets(), otelMetricsRawProcessorConfig.getCalculateExponentialHistogramBuckets(), flattenAttributesFlag));
        }
        recordsDroppedMetricsRawCounter.increment(droppedCounter.get());
        return recordsOut;
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
