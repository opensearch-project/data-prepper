/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.jetbrains.annotations.Nullable;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.http.OtlpHttpSender;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A Data Prepper Sink plugin that forwards spans to OTLP endpoint using
 */
@DataPrepperPlugin(
        name = "otlp",
        pluginType = Sink.class,
        pluginConfigurationType = OtlpSinkConfig.class
)
public class OtlpSink implements Sink<Record<Span>> {

    private static final Logger LOG = LoggerFactory.getLogger(OtlpSink.class);

    private final int batchSize;
    private final OtlpHttpSender httpSender;
    private final OTelProtoStandardCodec.OTelProtoEncoder encoder;
    private final OtlpSinkMetrics sinkMetrics;

    /**
     * Constructor for the OTLPSink plugin.
     *
     * @param config the configuration for the OTLP sink
     */
    @DataPrepperPluginConstructor
    public OtlpSink(@Nonnull final OtlpSinkConfig config, @Nonnull final PluginMetrics pluginMetrics) {
        this(config, pluginMetrics, null, null);
    }

    /**
     * Constructor for testing purposes.
     *
     * @param config     the configuration for the OTLP sink
     * @param encoder    OTEL Protobuf encoder to use
     * @param httpSender the HTTP sender to use
     */
    @VisibleForTesting
    OtlpSink(@Nonnull final OtlpSinkConfig config, @Nonnull final PluginMetrics pluginMetrics, final OTelProtoStandardCodec.OTelProtoEncoder encoder, final OtlpHttpSender httpSender) {
        this.batchSize = config.getBatchSize();
        this.sinkMetrics = new OtlpSinkMetrics(pluginMetrics);

        if (encoder == null && httpSender == null) {
            this.encoder = new OTelProtoStandardCodec.OTelProtoEncoder();
            this.httpSender = new OtlpHttpSender(config, sinkMetrics);
        } else {
            this.encoder = encoder;
            this.httpSender = httpSender;
        }

        LOG.info("Config setting: endpoint = {}", config.getEndpoint());
        LOG.info("Config setting: batch_size = {}", config.getBatchSize());
        LOG.info("Config setting: max_retries = {}", config.getMaxRetries());
        LOG.info("Config setting: aws_region = {}", config.getAwsRegion());
        LOG.info("Config setting: aws_sts_role_arn = {}", config.getStsRoleArn());
        LOG.info("Config setting: aws_sts_external_id = {}", config.getStsExternalId());
    }

    /**
     * Initializes the sink. Called once during pipeline startup.
     */
    @Override
    public void initialize() {
        LOG.info("Initialized OTLP Sink");
    }

    /**
     * Processes a batch of spans and sends them to the OTLP endpoint.
     *
     * @param records a collection of span records
     */
    @Override
    public void output(@Nonnull final Collection<Record<Span>> records) {
        sinkMetrics.incrementRecordsIn(records.size());

        final List<Record<Span>> recordList = new ArrayList<>(records);
        for (int i = 0; i < recordList.size(); i += this.batchSize) {
            final int end = Math.min(i + this.batchSize, recordList.size());
            final List<Record<Span>> batch = recordList.subList(i, end);

            try {
                final List<ResourceSpans> resourceSpans = batch.stream()
                        .map(Record::getData)
                        .map(this::getResourceSpans)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

                if (resourceSpans.isEmpty()) {
                    LOG.debug("Skipping empty span batch, nothing to send.");
                    continue;
                }

                final ExportTraceServiceRequest request = ExportTraceServiceRequest.newBuilder()
                        .addAllResourceSpans(resourceSpans)
                        .build();
                final byte[] payload = request.toByteArray();
                sinkMetrics.incrementPayloadSize(payload.length);

                httpSender.send(payload);
                LOG.info("Finished processing {} spans.", resourceSpans.size());

                sinkMetrics.incrementRecordsOut(batch.size());
            } catch (final RuntimeException e) {
                LOG.error("Unexpected error processing OTLP span batch: {}", e.getMessage(), e);

                sinkMetrics.incrementDroppedRecords(batch.size());
                sinkMetrics.incrementErrorsCount();
            }
        }
    }

    @Nullable
    private ResourceSpans getResourceSpans(final Span span) {
        try {
            return encoder.convertToResourceSpans(span);
        } catch (final Exception e) {
            LOG.warn("Failed to encode span with ID [{}], skipping.", span.getSpanId(), e);
            sinkMetrics.incrementErrorsCount();
            return null;
        }
    }

    /**
     * Indicates whether this sink is ready to receive data.
     *
     * @return true if the sink is ready
     */
    @Override
    public boolean isReady() {
        return true;
    }

    /**
     * Hook called during pipeline shutdown.
     */
    @Override
    public void shutdown() {
        // OkHttpClient is shared and self-managed; no need to explicitly close
        LOG.info("OTLP Sink shutdown complete");
    }

    /**
     * Records the latency between when each span originally started (as specified in the span's start time)
     * and when it was received by the sink (i.e., when this method is called).
     * <p>
     * This measures end-to-end ingestion latency from the span's source to the sink.
     * It does not include any processing or export latency within the sink itself.
     *
     * @param events A collection of spans received by the sink.
     */
    @Override
    public void updateLatencyMetrics(@Nonnull final Collection<Record<Span>> events) {
        final Instant now = Instant.now();

        for (final Record<Span> record : events) {
            try {
                final Instant startTime = Instant.parse(record.getData().getStartTime());
                final long durationMillis = Duration.between(startTime, now).toMillis();
                sinkMetrics.recordDeliveryLatency(durationMillis);
            } catch (final Exception e) {
                LOG.warn("Failed to parse startTime: {}", record.getData().getStartTime(), e);
                sinkMetrics.incrementErrorsCount();
            }
        }
    }
}
