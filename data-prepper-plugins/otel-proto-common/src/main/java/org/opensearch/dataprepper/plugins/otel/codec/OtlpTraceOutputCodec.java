/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import lombok.NonNull;
import org.apache.commons.codec.DecoderException;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.trace.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;

/**
 * An implementation of {@link OutputCodec} that converts {@link Span} events
 * into OpenTelemetry Protocol (OTLP) binary format using protobuf.
 * <p>
 * This codec is primarily intended for use with trace data that will be
 * forwarded to systems such as AWS X-Ray via OTLP.
 */
@DataPrepperPlugin(name = "otlp_trace", pluginType = OutputCodec.class)
public class OtlpTraceOutputCodec implements OutputCodec {

    private static final Logger LOG = LoggerFactory.getLogger(OtlpTraceOutputCodec.class);
    private static final String OTLP_EXTENSION = "pb";

    private final OTelProtoStandardCodec.OTelProtoEncoder encoder = new OTelProtoStandardCodec.OTelProtoEncoder();

    /**
     * Initializes the output stream. No-op for OTLP format.
     *
     * @param outputStream The output stream to write to.
     * @param event The event (not used).
     * @param context The codec context (not used).
     */
    @Override
    public void start(final OutputStream outputStream, final Event event, final OutputCodecContext context) {
        // No-op for OTLP format
    }

    /**
     * Writes a single {@link Span} event to the output stream in OTLP binary format.
     *
     * @param event The event to encode. Must be of type {@link Span}.
     * @param outputStream The stream to which the encoded bytes will be written.
     */
    @Override
    public void writeEvent(@NonNull final Event event, @NonNull final OutputStream outputStream) {
        if (!(event instanceof Span)) {
            throw new IllegalArgumentException("OtlpTraceOutputCodec only supports Span events");
        }

        final Span span = (Span) event;
        try {
            final ResourceSpans resourceSpans = encoder.convertToResourceSpans(span);
            final ExportTraceServiceRequest request = ExportTraceServiceRequest.newBuilder()
                    .addResourceSpans(resourceSpans)
                    .build();

            outputStream.write(request.toByteArray());
        } catch (final DecoderException e) {
            LOG.warn("Skipping invalid span with ID [{}] due to decoding error.", span.getSpanId(), e);
        } catch (final Exception e) {
            LOG.error("Unexpected error while writing span with ID [{}] to OTLP output.", span.getSpanId(), e);
        }
    }

    /**
     * Finalizes the output stream. No-op for OTLP format.
     *
     * @param outputStream The output stream to finalize.
     */
    @Override
    public void complete(final OutputStream outputStream) {
        // No-op for OTLP format
    }

    /**
     * Returns the file extension used by this codec.
     * In this case, "pb" for protobuf binary format.
     *
     * @return The string "pb".
     */
    @Override
    public String getExtension() {
        return OTLP_EXTENSION;
    }
}
