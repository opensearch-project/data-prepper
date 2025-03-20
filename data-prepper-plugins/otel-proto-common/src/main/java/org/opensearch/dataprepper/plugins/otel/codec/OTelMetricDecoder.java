/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.metric.Metric;


import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.function.Consumer;


public class OTelMetricDecoder implements ByteDecoder {
    private final OTelProtoCodec.OTelProtoDecoder otelProtoDecoder;
    public OTelMetricDecoder(OTelOutputFormat otelOutputFormat) {
        otelProtoDecoder = otelOutputFormat == OTelOutputFormat.OPENSEARCH ? new OTelProtoOpensearchCodec.OTelProtoDecoder() : new OTelProtoStandardCodec.OTelProtoDecoder();
    }
    public void parse(InputStream inputStream, Instant timeReceivedMs, Consumer<Record<Event>> eventConsumer) throws IOException {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.parseFrom(inputStream);
        AtomicInteger droppedCounter = new AtomicInteger(0);
        Collection<Record<? extends Metric>> records =
            otelProtoDecoder.parseExportMetricsServiceRequest(request, droppedCounter, OTelProtoCodec.DEFAULT_EXPONENTIAL_HISTOGRAM_MAX_ALLOWED_SCALE, timeReceivedMs, true, true, false);
        for (Record<? extends Metric> record: records) {
            eventConsumer.accept((Record)record);
        }
    }

}
