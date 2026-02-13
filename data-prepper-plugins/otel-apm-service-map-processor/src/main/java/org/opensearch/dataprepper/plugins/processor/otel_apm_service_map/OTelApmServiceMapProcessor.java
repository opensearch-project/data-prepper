/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.SingleThread;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.metric.JacksonMetric;
import org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import com.google.common.primitives.SignedBytes;
import org.apache.commons.codec.binary.Hex;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.ServiceConnection;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.ServiceOperationDetail;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.Service;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.Operation;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.SpanStateData;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ClientSpanDecoration;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ServerSpanDecoration;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ThreeWindowTraceData;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ThreeWindowTraceDataWithDecorations;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.EphemeralSpanDecorations;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.MetricKey;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.MetricAggregationState;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.utils.ApmServiceMapMetricsUtil;
import org.opensearch.dataprepper.plugins.processor.state.MapDbProcessorState;
import org.opensearch.dataprepper.model.annotations.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Experimental
@SingleThread
@DataPrepperPlugin(name = "otel_apm_service_map", pluginType = Processor.class,
        pluginConfigurationType = OTelApmServiceMapProcessorConfig.class)
public class OTelApmServiceMapProcessor extends AbstractProcessor<Record<Event>, Record<Event>> implements RequiresPeerForwarding {

    private static final String SPANS_DB_SIZE = "spansDbSize";
    private static final String SPANS_DB_COUNT = "spansDbCount";

    private static final Logger LOG = LoggerFactory.getLogger(OTelApmServiceMapProcessor.class);
    private static final String EVENT_TYPE_OTEL_APM_SERVICE_MAP = "SERVICE_MAP";
    private static final Collection<Record<Event>> EMPTY_COLLECTION = Collections.emptySet();
    private static final String SPAN_KIND_SERVER = "SPAN_KIND_SERVER";
    private static final String SPAN_KIND_CLIENT = "SPAN_KIND_CLIENT";

    // TODO: This should not be tracked in this class, move it up to the creator
    private static final AtomicInteger processorsCreated = new AtomicInteger(0);
    private static Instant previousTimestamp;
    private static Duration windowDuration;
    private static CyclicBarrier allThreadsCyclicBarrier;

    private static volatile MapDbProcessorState<Collection<SpanStateData>> previousWindow;
    private static volatile MapDbProcessorState<Collection<SpanStateData>> currentWindow;
    private static volatile MapDbProcessorState<Collection<SpanStateData>> nextWindow;
    private static File dbPath;
    private static Clock clock;

    private final int thisProcessorId;
    private final List<String> groupByAttributes;
    private final EventFactory eventFactory;

    @DataPrepperPluginConstructor
    public OTelApmServiceMapProcessor(
            final OTelApmServiceMapProcessorConfig config,
            final PluginMetrics pluginMetrics,
            final EventFactory eventFactory,
            final PipelineDescription pipelineDescription) {
        this(config.getWindowDuration(),
                new File(config.getDbPath()),
                Clock.systemUTC(),
                pipelineDescription.getNumberOfProcessWorkers(),
                eventFactory,
                pluginMetrics,
                config.getGroupByAttributes());
    }

    OTelApmServiceMapProcessor(final Duration windowDuration,
                               final File databasePath,
                               final Clock clock,
                               final int processWorkers,
                               final EventFactory eventFactory,
                               final PluginMetrics pluginMetrics) {
        this(windowDuration, databasePath, clock, processWorkers, eventFactory, pluginMetrics, Collections.emptyList());
    }

    OTelApmServiceMapProcessor(final Duration windowDuration,
                               final File databasePath,
                               final Clock clock,
                               final int processWorkers,
                               final EventFactory eventFactory,
                               final PluginMetrics pluginMetrics,
                               final List<String> groupByAttributes) {
        super(pluginMetrics);

        this.groupByAttributes = groupByAttributes != null ? Collections.unmodifiableList(groupByAttributes) : Collections.emptyList();

        this.eventFactory = eventFactory;
        OTelApmServiceMapProcessor.clock = clock;
        this.thisProcessorId = processorsCreated.getAndIncrement();

        if (isMasterInstance()) {
            previousTimestamp = OTelApmServiceMapProcessor.clock.instant();
            OTelApmServiceMapProcessor.windowDuration = windowDuration;
            OTelApmServiceMapProcessor.dbPath = createPath(databasePath);

            currentWindow = new MapDbProcessorState<>(dbPath, getNewDbName(), processWorkers);
            previousWindow = new MapDbProcessorState<>(dbPath, getNewDbName() + "-previous", processWorkers);
            nextWindow = new MapDbProcessorState<>(dbPath, getNewDbName() + "-next", processWorkers);

            allThreadsCyclicBarrier = new CyclicBarrier(processWorkers);
        }

        pluginMetrics.gauge(SPANS_DB_SIZE, this, processor -> processor.getSpansDbSize());
        pluginMetrics.gauge(SPANS_DB_COUNT, this, processor -> processor.getSpansDbCount());
    }

    /**
     * Adds the data for spans from the ResourceSpans object to the current window
     *
     * @param records Input records that will be modified/processed
     * @return If the window is reached, returns a list of ServiceDetails and ServiceRemoteDetails events.
     * Otherwise, returns an empty set.
     */
    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        final Collection<Record<Event>> apmEvents = windowDurationHasPassed() ? evaluateApmEvents() : EMPTY_COLLECTION;
        final Map<byte[], Collection<SpanStateData>> batchStateData = new TreeMap<>(SignedBytes.lexicographicalComparator());

        records.forEach(i -> processSpan((Span) i.getData(), batchStateData));

        try {
            // Update next window with batch data organized by traceId
            for (Map.Entry<byte[], Collection<SpanStateData>> entry : batchStateData.entrySet()) {
                final byte[] traceId = entry.getKey();
                final Collection<SpanStateData> spansForTrace = entry.getValue();

                Collection<SpanStateData> existingSpans = nextWindow.get(traceId);
                if (existingSpans == null) {
                    existingSpans = new HashSet<>();
                }
                existingSpans.addAll(spansForTrace);
                nextWindow.put(traceId, existingSpans);
            }
        } catch (RuntimeException e) {
            LOG.error("Caught exception trying to put batch state data", e);
        }
        return apmEvents;
    }

    public void prepareForShutdown() {
        previousTimestamp = Instant.EPOCH;
    }

    @Override
    public boolean isReadyForShutdown() {
        return currentWindow.size() == 0;
    }

    @Override
    public void shutdown() {
        previousWindow.delete();
        currentWindow.delete();
        if (nextWindow != null) {
            nextWindow.delete();
        }
        processorsCreated.set(0);
        allThreadsCyclicBarrier.reset();
    }

    /**
     * @return Spans database size in bytes
     */
    public double getSpansDbSize() {
        return currentWindow.sizeInBytes() + previousWindow.sizeInBytes() +
                (nextWindow != null ? nextWindow.sizeInBytes() : 0);
    }

    public double getSpansDbCount() {
        return currentWindow.size() + previousWindow.size() +
                (nextWindow != null ? nextWindow.size() : 0);
    }

    @Override
    public Collection<String> getIdentificationKeys() {
        return Collections.singleton("traceId");
    }

    /**
     * This function creates the directory if it doesn't exists and returns the File.
     *
     * @param path
     * @return path
     * @throws RuntimeException if the directory can not be created.
     */
    private static File createPath(File path) {
        if (!path.exists()) {
            if (!path.mkdirs()) {
                throw new RuntimeException(String.format("Unable to create the directory at the provided path: %s", path.getName()));
            }
        }
        return path;
    }

    private void processSpan(final Span span, final Map<byte[], Collection<SpanStateData>> batchStateData) {
        if (span.getServiceName() != null) {
            final String serviceName = span.getServiceName();
            final String spanId = span.getSpanId();
            final String traceId = span.getTraceId();
            final String parentSpanId = span.getParentSpanId();
            final String spanKind = span.getKind();
            final String spanName = span.getName();
            final String operation = span.getName();
            final Long durationInNanos = span.getDurationInNanos();
            final String status = extractSpanStatus(span);
            final String endTime = span.getEndTime();
            final Map<String, String> groupByAttrs = extractGroupByAttributes(span);
            final Map<String, Object> spanAttributes = extractSpanAttributes(span);

            try {
                final SpanStateData spanStateData = new SpanStateData(
                        serviceName,
                        spanId,
                        parentSpanId.isEmpty() ? null : parentSpanId,
                        traceId,
                        spanKind,
                        spanName,
                        operation,
                        durationInNanos,
                        status,
                        endTime,
                        groupByAttrs,
                        spanAttributes);

                Collection<SpanStateData> spansForTrace = batchStateData.computeIfAbsent(Hex.decodeHex(traceId),
                        k -> new HashSet<>());
                spansForTrace.add(spanStateData);
            } catch (Exception e) {
                LOG.error("Caught exception trying to put span state data into batch", e);
            }
        }
    }

    /**
     * Extract span status from the span's status field
     *
     * @param span The span to extract status from
     * @return String representation of the span status, or "OK" if not available
     */
    private String extractSpanStatus(final Span span) {
        try {
            final Map<String, Object> status = span.getStatus();
            if (status != null && status.containsKey("code")) {
                final Object code = status.get("code");
                if (code != null) {
                    return code.toString();
                }
            }
        } catch (Exception e) {
            LOG.debug("Error extracting span status: {}", e.getMessage());
        }
        return "OK"; // Default to OK if status is not available or extractable
    }

    /**
     * Extract span attributes including HTTP status codes and resource for error/fault/environment determination
     *
     * @param span The span to extract attributes from
     * @return Map of span attributes with resource information, or empty map if not available
     */
    private Map<String, Object> extractSpanAttributes(final Span span) {
        try {
            final Map<String, Object> combinedAttributes = new HashMap<>();

            final Map<String, Object> attributes = span.getAttributes();
            if (attributes != null) {
                combinedAttributes.putAll(attributes);
            }

            final Map<String, Object> resource = span.getResource();
            if (resource != null) {
                combinedAttributes.put("resource", resource);
            }
            final Map<String, Object> scope = span.getScope();
            if (scope != null ) {
                final Map<String, Object> scopeAttributes = (Map<String, Object>)scope.get("attributes");
                if (attributes != null) {
                    combinedAttributes.putAll(scopeAttributes);
                }
            }

            return combinedAttributes;
        } catch (Exception e) {
            LOG.debug("Error extracting span attributes: {}", e.getMessage());
            return Collections.emptyMap();
        }
    }

    /**
     * This method checks for master instance and let master instance process the current window and rotate the window.
     *
     * @return Set of Record<Event> containing json representation of ServiceConnection and ServiceOperationDetail found
     */
    private Collection<Record<Event>> evaluateApmEvents() {
        LOG.debug("Evaluating APM service map events with three-window semantics");
        try {
            allThreadsCyclicBarrier.await();

            Collection<Record<Event>> apmEvents = new HashSet<>();
            if (isMasterInstance()) {
                apmEvents = processCurrentWindowSpans();
                rotateWindows();
            }

            allThreadsCyclicBarrier.await();

            return apmEvents;
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Processes spans from the current window using three-window semantics (previous, current, next)
     * to generate APM service map events and metrics. The method operates in two main phases:
     * Phase 1: Decorates spans with ephemeral client/server relationship information using
     * two-pass decoration (CLIENT spans first, then SERVER spans with back-annotation).
     * Phase 2: Generates ServiceConnection and ServiceOperationDetail events from decorated
     * trace data, along with aggregated metrics for latency, throughput, and error rates.
     * The window logic ensures complete trace context by accessing spans across all three
     * time windows, while current window processing focuses on spans that belong to the
     * active processing window. Trace data decoration uses ephemeral storage that exists
     * only during this processing cycle to maintain span relationships and remote service
     * information. Event generation produces structured APM events and time-bucketed metrics
     * sorted chronologically for downstream consumption.
     */
    private Collection<Record<Event>> processCurrentWindowSpans() {
        final Collection<Record<Event>> apmEvents = new HashSet<>();
        final Instant currentTime = clock.instant();

        final EphemeralSpanDecorations ephemeralDecorations = new EphemeralSpanDecorations();

        final Map<MetricKey, MetricAggregationState> metricsStateByKey = new HashMap<>();

        final Map<String, Collection<SpanStateData>> previousSpansByTraceId = buildSpansByTraceIdMap(previousWindow);
        final Map<String, Collection<SpanStateData>> currentSpansByTraceId = buildSpansByTraceIdMap(currentWindow);
        final Map<String, Collection<SpanStateData>> nextSpansByTraceId = buildSpansByTraceIdMap(nextWindow);

        for (String traceId : currentSpansByTraceId.keySet()) {
            final ThreeWindowTraceDataWithDecorations traceData = buildThreeWindowTraceDataWithDecorations(
                    traceId, previousSpansByTraceId, currentSpansByTraceId, nextSpansByTraceId, ephemeralDecorations);

            if (!traceData.getProcessingSpans().isEmpty()) {
                decorateSpansInTraceWithEphemeralStorage(traceData);

                apmEvents.addAll(generateServiceConnectionsFromEphemeralDecorations(traceData, currentTime, metricsStateByKey));
                apmEvents.addAll(generateServiceOperationDetailsFromEphemeralDecorations(traceData, currentTime, metricsStateByKey));
            }
        }

        final List<JacksonMetric> metrics = ApmServiceMapMetricsUtil.createMetricsFromAggregatedState(metricsStateByKey);
        metrics.sort(Comparator.comparing(JacksonMetric::getTime));

        final List<Record<Event>> apmEventsSorted = new ArrayList<>();
        apmEventsSorted.addAll(metrics.stream().map(metric -> new Record<Event>(metric)).collect(Collectors.toList()));
        apmEventsSorted.addAll(apmEvents);

        return apmEventsSorted;
    }


    /**
     * Extract groupByAttributes from a span's resource attributes
     *
     * @param span The span to extract resource attributes from
     * @return Map of configured resource attributes or empty map if none configured/found
     */
    private Map<String, String> extractGroupByAttributes(final Span span) {
        if (groupByAttributes == null || groupByAttributes.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, String> result = new HashMap<>();

        try {
            final Map<String, Object> resource = span.getResource();
            if (resource == null) {
                return Collections.emptyMap();
            }

            final Object attributesObject = resource.get("attributes");
            if (!(attributesObject instanceof Map)) {
                return Collections.emptyMap();
            }

            @SuppressWarnings("unchecked")
            final Map<String, Object> resourceAttributes = (Map<String, Object>) attributesObject;

            for (String attrKey : groupByAttributes) {
                final Object value = resourceAttributes.get(attrKey);
                if (value != null) {
                    result.put(attrKey, value.toString());
                }
            }
        } catch (Exception e) {
            LOG.debug("Error extracting group by attributes from span resource: {}", e.getMessage());
        }

        return result.isEmpty() ? Collections.emptyMap() : result;
    }

    /**
     * Get anchor timestamp from span's endTime, truncated to minute boundary
     *
     * @param spanStateData The span to extract timestamp from
     * @param fallbackTime Current system time to use if span endTime is null
     * @return Instant truncated to the lower 1-minute boundary
     */
    private Instant getAnchorTimestampFromSpan(final SpanStateData spanStateData, final Instant fallbackTime) {
        Instant timestamp = fallbackTime; // Default to current system time

        final String endTime = spanStateData.getEndTime();
        try {
            if (endTime != null && !endTime.isEmpty()) {
                timestamp = Instant.parse(endTime);
            }
        } catch (Exception e) {
            LOG.debug("Failed to parse span endTime '{}', using fallback time: {}",
                     endTime, e.getMessage());
        }

        return timestamp.truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
    }

    /**
     * Rotate windows for processor state using three-window slot-machine semantics
     */
    private void rotateWindows() throws InterruptedException {
        LOG.debug("Rotating APM service map windows at " + clock.instant().toString());

        MapDbProcessorState<Collection<SpanStateData>> tempWindow = previousWindow;
        previousWindow = currentWindow;
        currentWindow = nextWindow;
        nextWindow = tempWindow;
        nextWindow.clear();

        previousTimestamp = clock.instant();
        LOG.debug("Done rotating APM service map windows - All metrics cleared for new window");
    }

    /**
     * @return Next database name
     */
    private String getNewDbName() {
        return "apm-db-" + clock.millis();
    }

    /**
     * @return Boolean indicating whether the window duration has lapsed
     */
    private boolean windowDurationHasPassed() {
        final Duration elapsed = Duration.between(previousTimestamp, clock.instant());
        return elapsed.compareTo(windowDuration) >= 0;
    }

    /**
     * Master instance is needed to do things like window rotation that should only be done once
     *
     * @return Boolean indicating whether this object is the master OTelApmServiceMapProcessor instance
     */
    private boolean isMasterInstance() {
        return thisProcessorId == 0;
    }

    /**
     * Build a map of traceId -> spans from a window
     *
     * @param window The window to extract spans from
     * @return Map of traceId to collection of spans
     */
    private Map<String, Collection<SpanStateData>> buildSpansByTraceIdMap(final MapDbProcessorState<Collection<SpanStateData>> window) {
        final Map<String, Collection<SpanStateData>> spansByTraceId = new HashMap<>();

        if (window != null && window.getAll() != null && window.size() > 0) {
            try {
                window.getIterator(processorsCreated.get(), thisProcessorId).forEachRemaining(entry -> {
                    final String traceId = Hex.encodeHexString(entry.getKey());
                    final Collection<SpanStateData> spans = entry.getValue();
                    if (spans != null && !spans.isEmpty()) {
                        spansByTraceId.put(traceId, spans);
                    }
                });
            } catch (NoSuchElementException e) {
                LOG.debug("Window is empty, skipping iteration: {}", e.getMessage());
            }
        }

        return spansByTraceId;
    }

    /**
     * Build three-window trace data for a specific trace
     *
     * @param traceId The trace ID
     * @param previousSpansByTraceId Previous window spans by trace ID
     * @param currentSpansByTraceId Current window spans by trace ID
     * @param nextSpansByTraceId next window spans by trace ID
     * @return ThreeWindowTraceData containing all necessary data for processing
     */
    private ThreeWindowTraceData buildThreeWindowTraceData(final String traceId,
                                                           final Map<String, Collection<SpanStateData>> previousSpansByTraceId,
                                                           final Map<String, Collection<SpanStateData>> currentSpansByTraceId,
                                                           final Map<String, Collection<SpanStateData>> nextSpansByTraceId) {
        final Collection<SpanStateData> previousSpans = previousSpansByTraceId.getOrDefault(traceId, Collections.emptyList());
        final Collection<SpanStateData> processingSpans = currentSpansByTraceId.getOrDefault(traceId, Collections.emptyList());
        final Collection<SpanStateData> nextSpans = nextSpansByTraceId.getOrDefault(traceId, Collections.emptyList());

        final Collection<SpanStateData> lookupSpans = new HashSet<>();
        lookupSpans.addAll(previousSpans);
        lookupSpans.addAll(processingSpans);
        lookupSpans.addAll(nextSpans);

        final Map<String, SpanStateData> spansBySpanId = new HashMap<>();
        final Map<String, Collection<SpanStateData>> childrenByParentId = new HashMap<>();
        final Set<String> processingSpanIds = new HashSet<>();

        for (SpanStateData spanStateData : lookupSpans) {
            final String spanId = spanStateData.getSpanId();
            spansBySpanId.put(spanId, spanStateData);

            if (spanStateData.getParentSpanId() != null) {
                final String parentSpanId = spanStateData.getParentSpanId();
                childrenByParentId.computeIfAbsent(parentSpanId, k -> new HashSet<>()).add(spanStateData);
            }
        }

        for (SpanStateData spanStateData : processingSpans) {
            processingSpanIds.add(spanStateData.getSpanId());
        }

        return new ThreeWindowTraceData(processingSpans, lookupSpans, spansBySpanId, childrenByParentId, processingSpanIds);
    }

    /**
     * Build three-window trace data with ephemeral decorations for a specific trace
     *
     * @param traceId The trace ID
     * @param previousSpansByTraceId Previous window spans by trace ID
     * @param currentSpansByTraceId Current window spans by trace ID
     * @param nextSpansByTraceId next window spans by trace ID
     * @param decorations Ephemeral decoration storage for this processing cycle
     * @return ThreeWindowTraceDataWithDecorations containing all necessary data for processing
     */
    private ThreeWindowTraceDataWithDecorations buildThreeWindowTraceDataWithDecorations(
            final String traceId,
            final Map<String, Collection<SpanStateData>> previousSpansByTraceId,
            final Map<String, Collection<SpanStateData>> currentSpansByTraceId,
            final Map<String, Collection<SpanStateData>> nextSpansByTraceId,
            final EphemeralSpanDecorations decorations) {

        final ThreeWindowTraceData baseTraceData = buildThreeWindowTraceData(
                traceId, previousSpansByTraceId, currentSpansByTraceId, nextSpansByTraceId);

        return new ThreeWindowTraceDataWithDecorations(
                baseTraceData.getProcessingSpans(),
                baseTraceData.getLookupSpans(),
                baseTraceData.getSpansBySpanId(),
                baseTraceData.getChildrenByParentId(),
                baseTraceData.getProcessingSpanIds(),
                decorations);
    }

    /**
     * PHASE 1: DECORATE SPANS with ephemeral storage - Two-pass decoration: first CLIENT spans, then SERVER spans
     *
     * This method performs span decoration in two explicit passes over all spans in the trace.
     * Pass 1: Decorate CLIENT spans with remote server information
     * Pass 2: Decorate SERVER spans and back-annotate CLIENT spans with parent server information
     *
     * @param traceData Three-window trace data with ephemeral decorations containing spans and indexes
     */
    private void decorateSpansInTraceWithEphemeralStorage(final ThreeWindowTraceDataWithDecorations traceData) {
        decorateClientSpansFirstPassWithEphemeralStorage(traceData);
        decorateServerSpansSecondPassWithEphemeralStorage(traceData);
    }

    /**
     * First pass: decorate CLIENT spans with child SERVER span information using ephemeral storage
     * Traverse ALL CLIENT spans in the trace and find their child SERVER spans (remote servers)
     *
     * @param traceData Three-window trace data with ephemeral decorations containing spans and indexes
     */
    private void decorateClientSpansFirstPassWithEphemeralStorage(final ThreeWindowTraceDataWithDecorations traceData) {
        for (SpanStateData clientSpan : traceData.getLookupSpans()) {
            if (SPAN_KIND_CLIENT.equals(clientSpan.getSpanKind())) {
                final String clientSpanId = clientSpan.getSpanId();
                final Collection<SpanStateData> childServerSpans = traceData.getChildrenByParentId().getOrDefault(clientSpanId, Collections.emptyList())
                        .stream()
                        .filter(span -> SPAN_KIND_SERVER.equals(span.getSpanKind()))
                        .collect(java.util.stream.Collectors.toList());

                String remoteService = "unknown";
                String remoteOperation = "unknown";
                String remoteEnvironment = "generic:default"; // Default environment string
                Map<String, String> remoteGroupByAttributes = Collections.emptyMap();

                if (!childServerSpans.isEmpty()) {
                    final SpanStateData childServerSpan = childServerSpans.iterator().next();
                    remoteService = childServerSpan.getServiceName();
                    remoteOperation = childServerSpan.getOperationName();
                    remoteEnvironment = childServerSpan.getEnvironment();
                    remoteGroupByAttributes = childServerSpan.getGroupByAttributes();
                }

                final ClientSpanDecoration decoration = new ClientSpanDecoration(
                        null,
                        remoteEnvironment,
                        remoteService,
                        remoteOperation,
                        remoteGroupByAttributes
                );
                traceData.getDecorations().setClientDecoration(clientSpanId, decoration);
            }
        }
    }

    /**
     * Second pass: decorate SERVER spans and back-annotate CLIENT spans with parent server information using ephemeral storage
     * Traverse ALL SERVER spans in the trace and find their descendant CLIENT spans from same service
     *
     * @param traceData Three-window trace data with ephemeral decorations containing spans and indexes
     */
    private void decorateServerSpansSecondPassWithEphemeralStorage(final ThreeWindowTraceDataWithDecorations traceData) {
        for (SpanStateData serverSpan : traceData.getLookupSpans()) {
            if (SPAN_KIND_SERVER.equals(serverSpan.getSpanKind())) {
                final Collection<SpanStateData> clientDescendants = findClientDescendantsForServerThreeWindow(serverSpan, traceData);

                final ServerSpanDecoration serverDecoration = new ServerSpanDecoration(clientDescendants);
                traceData.getDecorations().setServerDecoration(serverSpan.getSpanId(), serverDecoration);

                for (SpanStateData clientSpan : clientDescendants) {
                    final String clientSpanId = clientSpan.getSpanId();
                    final ClientSpanDecoration existingDecoration = traceData.getDecorations().getClientDecoration(clientSpanId);

                    if (existingDecoration != null) {
                        final ClientSpanDecoration updatedDecoration = new ClientSpanDecoration(
                                serverSpan.getOperationName(),
                                existingDecoration.getRemoteEnvironment(),
                                existingDecoration.getRemoteService(),
                                existingDecoration.getRemoteOperation(),
                                existingDecoration.getRemoteGroupByAttributes()
                        );
                        traceData.getDecorations().setClientDecoration(clientSpanId, updatedDecoration);
                    } else {
                        final ClientSpanDecoration newDecoration = new ClientSpanDecoration(
                                serverSpan.getOperationName(),
                                clientSpan.getEnvironment(),
                                "unknown",
                                "unknown",
                                Collections.emptyMap()
                        );
                        traceData.getDecorations().setClientDecoration(clientSpanId, newDecoration);
                    }
                }
            }
        }
    }

    /**
     * PHASE 2: Generate ServiceConnection events and CLIENT-side metrics from ephemeral decorations
     * Uses only ephemeral decoration data - no relationship computation
     *
     * @param traceData Three-window trace data with ephemeral decorations (only processing spans are used)
     * @param currentTime Current timestamp
     * @param metricsStateByKey Shared map for metric aggregation across all traces
     * @return Collection of ServiceConnection events
     */
    private Collection<Record<Event>> generateServiceConnectionsFromEphemeralDecorations(final ThreeWindowTraceDataWithDecorations traceData,
                                                                                          final Instant currentTime,
                                                                                          final Map<MetricKey, MetricAggregationState> metricsStateByKey) {
        final Collection<Record<Event>> connectionEvents = new HashSet<>();

        for (SpanStateData clientSpan : traceData.getProcessingSpans()) {
            if (SPAN_KIND_CLIENT.equals(clientSpan.getSpanKind())) {
                final ClientSpanDecoration decoration = traceData.getDecorations().getClientDecoration(clientSpan.getSpanId());

                if (decoration != null && !"unknown".equals(decoration.getRemoteService())) {
                    final Service clientService = new Service(
                            new Service.KeyAttributes(clientSpan.getEnvironment(), clientSpan.getServiceName()),
                            clientSpan.getGroupByAttributes()
                    );

                    final Service serverService = new Service(
                            new Service.KeyAttributes(decoration.getRemoteEnvironment(), decoration.getRemoteService()),
                            decoration.getRemoteGroupByAttributes()
                    );

                    final Instant connectionAnchorTimestamp = getAnchorTimestampFromSpan(clientSpan, currentTime);

                    final ServiceConnection serviceConnection = new ServiceConnection(
                            clientService,
                            serverService,
                            connectionAnchorTimestamp
                    );

                    final EventMetadata eventMetadata = new DefaultEventMetadata.Builder()
                        .withEventType(EVENT_TYPE_OTEL_APM_SERVICE_MAP).build();

                    final Event connectionEvent = eventFactory.eventBuilder(EventBuilder.class)
                                .withEventMetadata(eventMetadata)
                                .withData(serviceConnection)
                            .build();

                    connectionEvents.add(new Record<>(connectionEvent));

                    if (decoration.getParentServerOperationName() != null) {
                        final Instant metricsAnchorTimestamp = getAnchorTimestampFromSpan(clientSpan, currentTime);
                        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(clientSpan, decoration, currentTime, metricsStateByKey, metricsAnchorTimestamp);
                    }
                }
            }
        }

        return connectionEvents;
    }

    /**
     * PHASE 2: Generate ServiceOperationDetail events and metrics from ephemeral decorations
     * Uses only ephemeral decoration data - no relationship computation
     *
     * @param traceData Three-window trace data with ephemeral decorations (only processing spans are used)
     * @param currentTime Current timestamp
     * @param metricsStateByKey Shared map for metric aggregation across all traces
     * @return Collection of ServiceOperationDetail events
     */
    private Collection<Record<Event>> generateServiceOperationDetailsFromEphemeralDecorations(final ThreeWindowTraceDataWithDecorations traceData,
                                                                                               final Instant currentTime,
                                                                                               final Map<MetricKey, MetricAggregationState> metricsStateByKey) {
        final Collection<Record<Event>> operationEvents = new HashSet<>();

        for (SpanStateData serverSpan : traceData.getProcessingSpans()) {
            if (SPAN_KIND_SERVER.equals(serverSpan.getSpanKind())) {
                final ServerSpanDecoration decoration = traceData.getDecorations().getServerDecoration(serverSpan.getSpanId());

                final Instant anchorTimestamp = getAnchorTimestampFromSpan(serverSpan, currentTime);
                ApmServiceMapMetricsUtil.generateMetricsForServerSpan(serverSpan, currentTime, metricsStateByKey, anchorTimestamp);

                if (decoration != null && !decoration.getClientDescendants().isEmpty()) {
                    for (SpanStateData clientSpan : decoration.getClientDescendants()) {
                        final ClientSpanDecoration clientDecoration = traceData.getDecorations().getClientDecoration(clientSpan.getSpanId());

                        if (clientDecoration != null) {
                            final Service service = new Service(
                                    new Service.KeyAttributes(serverSpan.getEnvironment(), serverSpan.getServiceName()),
                                    serverSpan.getGroupByAttributes()
                            );

                            final Service remoteService = new Service(
                                    new Service.KeyAttributes(clientDecoration.getRemoteEnvironment(), clientDecoration.getRemoteService()),
                                    clientDecoration.getRemoteGroupByAttributes()
                            );

                            final Operation operation = new Operation(
                                    serverSpan.getOperationName(),
                                    remoteService,
                                    clientDecoration.getRemoteOperation()
                            );

                            final Instant operationAnchorTimestamp = getAnchorTimestampFromSpan(serverSpan, currentTime);

                            final ServiceOperationDetail serviceOperationDetail = new ServiceOperationDetail(
                                    service,
                                    operation,
                                    operationAnchorTimestamp
                            );

                            final EventMetadata eventMetadata = new DefaultEventMetadata.Builder()
                                .withEventType(EVENT_TYPE_OTEL_APM_SERVICE_MAP).build();

                            final Event operationEvent = eventFactory.eventBuilder(EventBuilder.class)
                                        .withEventMetadata(eventMetadata)
                                        .withData(serviceOperationDetail)
                                    .build();
                            operationEvents.add(new Record<>(operationEvent));
                        }
                    }
                } else {
                    final Service service = new Service(
                            new Service.KeyAttributes(serverSpan.getEnvironment(), serverSpan.getServiceName()),
                            serverSpan.getGroupByAttributes()
                    );

                    final Operation operation = new Operation(
                            serverSpan.getOperationName(),
                            null,
                            null
                    );

                    final Instant unknownAnchorTimestamp = getAnchorTimestampFromSpan(serverSpan, currentTime);

                    final ServiceOperationDetail serviceOperationDetail = new ServiceOperationDetail(
                            service,
                            operation,
                            unknownAnchorTimestamp
                    );

                    final EventMetadata eventMetadata = new DefaultEventMetadata.Builder()
                        .withEventType(EVENT_TYPE_OTEL_APM_SERVICE_MAP).build();

                    final Event operationEvent = eventFactory.eventBuilder(EventBuilder.class)
                                .withEventMetadata(eventMetadata)
                                .withData(serviceOperationDetail)
                            .build();
                    operationEvents.add(new Record<>(operationEvent));
                }
            }
        }

        return operationEvents;
    }

    /**
     * Find CLIENT descendant spans from the same service as the SERVER span using three-window semantics
     * Uses BFS with pruning - stops traversing when service name changes
     *
     * @param serverSpan The SERVER span
     * @param traceData Three-window trace data
     * @return Collection of CLIENT descendant spans from the same service
     */
    private Collection<SpanStateData> findClientDescendantsForServerThreeWindow(final SpanStateData serverSpan,
                                                                                final ThreeWindowTraceData traceData) {
        final Collection<SpanStateData> clientDescendants = new HashSet<>();
        final String serverSpanId = serverSpan.getSpanId();

        final Set<String> visited = new HashSet<>();
        final java.util.Queue<String> queue = new java.util.LinkedList<>();
        queue.offer(serverSpanId);
        visited.add(serverSpanId);

        while (!queue.isEmpty()) {
            final String currentSpanId = queue.poll();
            final Collection<SpanStateData> children = traceData.getChildrenByParentId().getOrDefault(currentSpanId, Collections.emptyList());

            for (SpanStateData child : children) {
                final String childSpanId = child.getSpanId();

                if (!visited.contains(childSpanId)) {
                    visited.add(childSpanId);

                    if (serverSpan.getServiceName().equals(child.getServiceName())) {
                        if (SPAN_KIND_CLIENT.equals(child.getSpanKind())) {
                            clientDescendants.add(child);
                        }

                        queue.offer(childSpanId);
                    }
                }
            }
        }
        return clientDescendants;
    }
}
