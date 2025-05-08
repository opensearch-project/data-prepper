/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.server;

import com.linecorp.armeria.server.ServiceRequestContext;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.exceptions.RequestCancelledException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec.DEFAULT_EXPONENTIAL_HISTOGRAM_MAX_ALLOWED_SCALE;

public class TestService extends MetricsServiceGrpc.MetricsServiceImplBase {
    private static final Logger LOG = LoggerFactory.getLogger(TestService.class);

    private final int bufferWriteTimeoutInMillis;
    private final OTelProtoCodec.OTelProtoDecoder oTelProtoDecoder;
    private final Buffer<Record<? extends Metric>> buffer;



    public TestService(int bufferWriteTimeoutInMillis,
                       final OTelProtoCodec.OTelProtoDecoder oTelProtoDecoder,
                       Buffer<Record<? extends Metric>> buffer,
                       final PluginMetrics pluginMetrics) {
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
        this.buffer = buffer;

        this.oTelProtoDecoder = oTelProtoDecoder;
    }

    @Override
    public void export(final ExportMetricsServiceRequest request, final StreamObserver<ExportMetricsServiceResponse> responseObserver) {

        if (ServiceRequestContext.current().isTimedOut()) {
            return;
        }

        if (Context.current().isCancelled()) {
            throw new RequestCancelledException("Cancelled by client");
        }

        processRequest(request, responseObserver);
    }

    private void processRequest(final ExportMetricsServiceRequest request, final StreamObserver<ExportMetricsServiceResponse> responseObserver) {
        try {
            if (buffer.isByteBuffer()) {
                buffer.writeBytes(request.toByteArray(), null, bufferWriteTimeoutInMillis);
            } else {
                Collection<Record<? extends Metric>> metrics;

                AtomicInteger droppedCounter = new AtomicInteger(0);
                metrics = oTelProtoDecoder.parseExportMetricsServiceRequest(request, droppedCounter, DEFAULT_EXPONENTIAL_HISTOGRAM_MAX_ALLOWED_SCALE, Instant.now(), true, true, true);
                buffer.writeAll(metrics, bufferWriteTimeoutInMillis);
            }
        } catch (Exception e) {
            if (ServiceRequestContext.current().isTimedOut()) {
                LOG.warn("Exception writing to buffer but request already timed out.", e);
                return;
            }

            LOG.error("Failed to write the request of size {} due to:", request.toString().length(), e);
            throw new BufferWriteException(e.getMessage(), e);
        }

        if (ServiceRequestContext.current().isTimedOut()) {
            LOG.warn("Buffer write completed successfully but request already timed out.");
            return;
        }

        responseObserver.onNext(ExportMetricsServiceResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
