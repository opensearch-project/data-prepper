package org.opensearch.dataprepper.plugins.source.oteltrace.http;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.opensearch.dataprepper.exceptions.BadRequestException;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.logging.DataPrepperMarkers;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.Consumes;
import com.linecorp.armeria.server.annotation.Post;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;

public class ArmeriaHttpService {
    private static final Logger LOG = LoggerFactory.getLogger(ArmeriaHttpService.class);

    public static final String REQUEST_TIMEOUTS = "requestTimeouts";
    public static final String REQUESTS_RECEIVED = "requestsReceived";
    public static final String BAD_REQUESTS = "badRequests";
    public static final String REQUESTS_TOO_LARGE = "requestsTooLarge";
    public static final String INTERNAL_SERVER_ERROR = "internalServerError";
    public static final String SUCCESS_REQUESTS = "successRequests";
    public static final String PAYLOAD_SIZE = "payloadSize";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";

    private final OTelProtoCodec.OTelProtoDecoder oTelProtoDecoder;
    private final Buffer<Record<Object>> buffer;

    private final int bufferWriteTimeoutInMillis;

    private final Counter requestsReceivedCounter;
    private final Counter successRequestsCounter;
    private final DistributionSummary payloadSizeSummary;
    private final Timer requestProcessDuration;

    public ArmeriaHttpService(Buffer<Record<Object>> buffer, final PluginMetrics pluginMetrics, final int bufferWriteTimeoutInMillis) {
        this.buffer = buffer;
        this.oTelProtoDecoder = new OTelProtoCodec.OTelProtoDecoder();
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;

        // todo tlongo encapsulate into own class, since both, grpc and http, should contribute to those
        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
        successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
        payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE);
        requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION);
    }

    // todo tlongo healthcheck?

    @Post("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
    @Consumes(value = "application/json")
    public ExportTraceServiceResponse exportTrace(ExportTraceServiceRequest request) {
        requestsReceivedCounter.increment();
        payloadSizeSummary.record(request.getSerializedSize());

        requestProcessDuration.record(() -> processRequest(request));

        return ExportTraceServiceResponse.newBuilder().build();
    }

    // todo tlongo exract in order to be used by http and grpc?
    private void processRequest(final ExportTraceServiceRequest request) {
        final Collection<Span> spans;

        try {
            spans = oTelProtoDecoder.parseExportTraceServiceRequest(request, Instant.now());
        } catch (final Exception e) {
            LOG.warn(DataPrepperMarkers.SENSITIVE, "Failed to parse request with error '{}'. Request body: {}.", e.getMessage(), request);
            throw new BadRequestException(e.getMessage(), e);
        }

        try {
            if (buffer.isByteBuffer()) {
                Map<String, ExportTraceServiceRequest> requestsMap = oTelProtoDecoder.splitExportTraceServiceRequestByTraceId(request);
                for (Map.Entry<String, ExportTraceServiceRequest> entry: requestsMap.entrySet()) {
                    buffer.writeBytes(entry.getValue().toByteArray(), entry.getKey(), bufferWriteTimeoutInMillis);
                }
            } else {
                final List<Record<Object>> records = spans.stream().map(span -> new Record<Object>(span)).collect(Collectors.toList());
                buffer.writeAll(records, bufferWriteTimeoutInMillis);
            }
        } catch (final Exception e) {
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

        successRequestsCounter.increment();

        // todo tlongo what is the responseObserver used for?
//        responseObserver.onNext(ExportTraceServiceResponse.newBuilder().build());
//        responseObserver.onCompleted();
    }
}
