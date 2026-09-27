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
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.Node;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.NodeOperationDetail;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.Operation;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.SpanStateData;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ClientSpanDecoration;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.DependencyCardinalityLimiter;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.DependencyNamingPolicy;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ServerSpanDecoration;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ThreeWindowTraceData;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ThreeWindowTraceDataWithDecorations;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.EphemeralSpanDecorations;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.MetricKey;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.MetricAggregationState;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.utils.ApmServiceMapMetricsUtil;
import org.opensearch.dataprepper.plugins.processor.state.MapDbProcessorState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import org.opensearch.dataprepper.model.host.HostContext;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.DependencyCardinalityLimiter.OTHER_REMOTE_SERVICE;
import static org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.SpanStateData.isPublishableDependencyName;

@SingleThread
@DataPrepperPlugin(name = "otel_apm_service_map", pluginType = Processor.class,
        pluginConfigurationType = OTelApmServiceMapProcessorConfig.class)
public class OTelApmServiceMapProcessor extends AbstractProcessor<Record<Event>, Record<Event>> implements RequiresPeerForwarding {

    private static final String SPANS_DB_SIZE = "spansDbSize";
    private static final String SPANS_DB_COUNT = "spansDbCount";
    static final String DEPENDENCY_OVERFLOW_METRIC = "dependencyNodesOverflowed";
    static final String REMOTE_OPERATION_OVERFLOW_METRIC = "dependencyRemoteOperationsOverflowed";

    private static final Logger LOG = LoggerFactory.getLogger(OTelApmServiceMapProcessor.class);
    private static final String EVENT_TYPE_OTEL_APM_SERVICE_MAP = "SERVICE_MAP";
    private static final Collection<Record<Event>> EMPTY_COLLECTION = Collections.emptySet();
    private static final String SPAN_KIND_SERVER = "SPAN_KIND_SERVER";
    private static final String SPAN_KIND_CLIENT = "SPAN_KIND_CLIENT";
    private static final String SPAN_KIND_PRODUCER = "SPAN_KIND_PRODUCER";
    private static final String SPAN_KIND_CONSUMER = "SPAN_KIND_CONSUMER";
    // Single source of truth for node-type strings lives in SpanStateData.
    private static final String NODE_TYPE_SERVICE = SpanStateData.NODE_TYPE_SERVICE;
    private static final String NODE_TYPE_MESSAGING = SpanStateData.NODE_TYPE_MESSAGING;
    private static final String NODE_TYPE_EXTERNAL = SpanStateData.NODE_TYPE_EXTERNAL;
    // Brokers are shared infrastructure: producers and consumers in different environments must reach
    // the same broker node, so broker nodes use one fixed environment instead of the span's.
    static final String MESSAGING_BROKER_ENVIRONMENT = "generic:default";
    private static final String MESSAGING_OPERATION_PROCESS = "process";

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
    private final String hostId;
    private final List<String> groupByAttributes;
    private final MetricTimestampSource metricTimestampSource;
    private final MetricTimestampGranularity metricTimestampGranularity;
    private final EventFactory eventFactory;
    private final boolean dependencyNodesEnabled;
    // Null when dependency nodes are disabled, so spans skip dependency derivation entirely.
    private final DependencyNamingPolicy dependencyNamingPolicy;
    private final int maxDependenciesPerService;
    private final int maxRemoteOperationsPerService;
    private final AtomicLong dependencyOverflowTotal = new AtomicLong();
    private final AtomicLong remoteOperationOverflowTotal = new AtomicLong();

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
                config.getGroupByAttributes(),
                config.getMetricTimestampSource(),
                config.getMetricTimestampGranularity(),
                config.getDependencyNodes());
    }

    OTelApmServiceMapProcessor(final Duration windowDuration,
                               final File databasePath,
                               final Clock clock,
                               final int processWorkers,
                               final EventFactory eventFactory,
                               final PluginMetrics pluginMetrics) {
        this(windowDuration, databasePath, clock, processWorkers, eventFactory, pluginMetrics,
                Collections.emptyList(), MetricTimestampSource.SPAN_END_TIME, MetricTimestampGranularity.SECONDS);
    }

    OTelApmServiceMapProcessor(final Duration windowDuration,
                               final File databasePath,
                               final Clock clock,
                               final int processWorkers,
                               final EventFactory eventFactory,
                               final PluginMetrics pluginMetrics,
                               final List<String> groupByAttributes) {
        this(windowDuration, databasePath, clock, processWorkers, eventFactory, pluginMetrics,
                groupByAttributes, MetricTimestampSource.SPAN_END_TIME, MetricTimestampGranularity.SECONDS);
    }

    OTelApmServiceMapProcessor(final Duration windowDuration,
                               final File databasePath,
                               final Clock clock,
                               final int processWorkers,
                               final EventFactory eventFactory,
                               final PluginMetrics pluginMetrics,
                               final List<String> groupByAttributes,
                               final MetricTimestampSource metricTimestampSource) {
        this(windowDuration, databasePath, clock, processWorkers, eventFactory, pluginMetrics,
                groupByAttributes, metricTimestampSource, MetricTimestampGranularity.SECONDS);
    }

    OTelApmServiceMapProcessor(final Duration windowDuration,
                               final File databasePath,
                               final Clock clock,
                               final int processWorkers,
                               final EventFactory eventFactory,
                               final PluginMetrics pluginMetrics,
                               final List<String> groupByAttributes,
                               final MetricTimestampSource metricTimestampSource,
                               final MetricTimestampGranularity metricTimestampGranularity) {
        this(windowDuration, databasePath, clock, processWorkers, eventFactory, pluginMetrics,
                groupByAttributes, metricTimestampSource, metricTimestampGranularity, new DependencyNodesConfig());
    }

    OTelApmServiceMapProcessor(final Duration windowDuration,
                               final File databasePath,
                               final Clock clock,
                               final int processWorkers,
                               final EventFactory eventFactory,
                               final PluginMetrics pluginMetrics,
                               final List<String> groupByAttributes,
                               final MetricTimestampSource metricTimestampSource,
                               final MetricTimestampGranularity metricTimestampGranularity,
                               final DependencyNodesConfig dependencyNodesConfig) {
        super(pluginMetrics);

        this.hostId = HostContext.getStableHostId();
        this.groupByAttributes = groupByAttributes != null ? Collections.unmodifiableList(groupByAttributes) : Collections.emptyList();
        this.metricTimestampSource = metricTimestampSource != null ? metricTimestampSource : MetricTimestampSource.ARRIVAL_TIME;
        this.metricTimestampGranularity = metricTimestampGranularity != null ? metricTimestampGranularity : MetricTimestampGranularity.SECONDS;

        this.eventFactory = eventFactory;
        final DependencyNodesConfig dependencyNodes = dependencyNodesConfig != null
                ? dependencyNodesConfig : new DependencyNodesConfig();
        this.dependencyNodesEnabled = dependencyNodes.isEnabled();
        this.dependencyNamingPolicy = dependencyNodesEnabled
                ? new DependencyNamingPolicy(dependencyNodes.getHostnameDenylistPatterns()) : null;
        this.maxDependenciesPerService = dependencyNodes.getMaxDependenciesPerService();
        this.maxRemoteOperationsPerService = dependencyNodes.getMaxRemoteOperationsPerService();
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
        if (dependencyNodesEnabled) {
            // Cumulative totals of dependency calls collapsed by the cardinality caps.
            pluginMetrics.gauge(DEPENDENCY_OVERFLOW_METRIC, dependencyOverflowTotal);
            pluginMetrics.gauge(REMOTE_OPERATION_OVERFLOW_METRIC, remoteOperationOverflowTotal);
        }
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
                        spanAttributes,
                        dependencyNamingPolicy);

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
            if (scope != null) {
                final Map<String, Object> scopeAttributes = (Map<String, Object>)scope.get("attributes");
                if (scopeAttributes != null) {
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
     * @return Set of Record<Event> containing json representation of NodeOperationDetail found
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
     * to generate APM service map events and metrics. The method operates in two phases:
     *
     * Phase 1: Decorates spans with ephemeral client/server relationship information using
     * two-pass decoration (CLIENT spans first, then SERVER spans with back-annotation).
     *
     * Phase 2: Generates NodeOperationDetail events using CLIENT-span-primary algorithm.
     * CLIENT spans are the primary emission source since their decoration contains all needed
     * data (sourceNode, targetNode, sourceOperation from parentServerOperationName, targetOperation
     * from remoteOperation). Leaf SERVER spans (no CLIENT descendants) emit separately.
     * Metrics (latency, throughput, error rates) are generated for all SERVER spans and for
     * CLIENT spans with full operation context.
     */
    private Collection<Record<Event>> processCurrentWindowSpans() {
        final Collection<Record<Event>> apmEvents = new HashSet<>();
        final Instant currentTime = clock.instant();

        final EphemeralSpanDecorations ephemeralDecorations = new EphemeralSpanDecorations();

        final Map<MetricKey, MetricAggregationState> sumStateByKey = new HashMap<>();
        final Map<MetricKey, MetricAggregationState> histogramStateByKey = new HashMap<>();
        final Set<NodeOperationDetail> dedupedNodeDetails = new HashSet<>();

        final Map<String, Collection<SpanStateData>> previousSpansByTraceId = buildSpansByTraceIdMap(previousWindow);
        final Map<String, Collection<SpanStateData>> currentSpansByTraceId = buildSpansByTraceIdMap(currentWindow);
        final Map<String, Collection<SpanStateData>> nextSpansByTraceId = buildSpansByTraceIdMap(nextWindow);

        final Set<String> knownServerServices = dependencyNodesEnabled
                ? collectServerServiceNames(previousSpansByTraceId, currentSpansByTraceId, nextSpansByTraceId)
                : Collections.emptySet();
        final DependencyCardinalityLimiter dependencyLimiter =
                new DependencyCardinalityLimiter(maxDependenciesPerService, maxRemoteOperationsPerService);

        for (String traceId : currentSpansByTraceId.keySet()) {
            final ThreeWindowTraceDataWithDecorations traceData = buildThreeWindowTraceDataWithDecorations(
                    traceId, previousSpansByTraceId, currentSpansByTraceId, nextSpansByTraceId, ephemeralDecorations);

            if (!traceData.getProcessingSpans().isEmpty()) {
                decorateSpansInTraceWithEphemeralStorage(traceData, knownServerServices);

                generateNodeOperationDetailEvents(traceData, currentTime, sumStateByKey, histogramStateByKey,
                        dedupedNodeDetails, dependencyLimiter);
            }
        }
        dependencyOverflowTotal.addAndGet(dependencyLimiter.getDependencyOverflowCount());
        remoteOperationOverflowTotal.addAndGet(dependencyLimiter.getRemoteOperationOverflowCount());

        // Convert deduped NodeOperationDetails to events
        for (NodeOperationDetail detail : dedupedNodeDetails) {
            final EventMetadata eventMetadata = new DefaultEventMetadata.Builder()
                    .withEventType(EVENT_TYPE_OTEL_APM_SERVICE_MAP).build();

            final Event event = eventFactory.eventBuilder(EventBuilder.class)
                    .withEventMetadata(eventMetadata)
                    .withData(detail)
                    .build();

            apmEvents.add(new Record<>(event));
        }

        final List<JacksonMetric> metrics = ApmServiceMapMetricsUtil.createMetricsFromAggregatedState(sumStateByKey, histogramStateByKey);
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
     * Get anchor timestamp for metrics, truncated to the specified unit.
     * When metric_timestamp_source is ARRIVAL_TIME, uses fallbackTime (clock.instant()).
     * When metric_timestamp_source is SPAN_END_TIME, uses the span's endTime field.
     *
     * @param spanStateData The span to extract timestamp from
     * @param fallbackTime Current system time to use as arrival time or if span endTime is null
     * @param truncationUnit The ChronoUnit to truncate the timestamp to
     * @return Instant truncated to the specified boundary
     */
    private Instant getAnchorTimestampFromSpan(final SpanStateData spanStateData, final Instant fallbackTime,
                                               final ChronoUnit truncationUnit) {
        if (metricTimestampSource == MetricTimestampSource.ARRIVAL_TIME) {
            return fallbackTime.truncatedTo(truncationUnit);
        }

        // SPAN_END_TIME mode: parse span's endTime, fall back to system time
        Instant timestamp = fallbackTime;
        final String endTime = spanStateData.getEndTime();
        try {
            if (endTime != null && !endTime.isEmpty()) {
                timestamp = Instant.parse(endTime);
            }
        } catch (Exception e) {
            LOG.debug("Failed to parse span endTime '{}', using fallback time: {}",
                     endTime, e.getMessage());
        }

        return timestamp.truncatedTo(truncationUnit);
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
    private void decorateSpansInTraceWithEphemeralStorage(final ThreeWindowTraceDataWithDecorations traceData,
                                                          final Set<String> knownServerServices) {
        decorateClientSpansFirstPassWithEphemeralStorage(traceData, knownServerServices);
        decorateServerSpansSecondPassWithEphemeralStorage(traceData);
    }

    /**
     * First pass: decorate CLIENT spans with child SERVER span information using ephemeral storage
     * Traverse ALL CLIENT spans in the trace and find their child SERVER spans (remote servers)
     *
     * @param traceData Three-window trace data with ephemeral decorations containing spans and indexes
     * @param knownServerServices Lower-cased names of services seen with a SERVER span in the three windows
     */
    private void decorateClientSpansFirstPassWithEphemeralStorage(final ThreeWindowTraceDataWithDecorations traceData,
                                                                  final Set<String> knownServerServices) {
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
                String remoteNodeType = NODE_TYPE_SERVICE;
                Map<String, String> remoteDependencyAttributes = Collections.emptyMap();

                if (!childServerSpans.isEmpty()) {
                    final SpanStateData childServerSpan = childServerSpans.iterator().next();
                    remoteService = childServerSpan.getServiceName();
                    remoteOperation = childServerSpan.getOperationName();
                    remoteEnvironment = childServerSpan.getEnvironment();
                    remoteGroupByAttributes = childServerSpan.getGroupByAttributes();
                } else if (dependencyNodesEnabled
                        && isSynthesizedDependencyTarget(clientSpan, traceData, knownServerServices)) {
                    // No downstream SERVER span: this CLIENT call targets an external dependency
                    // (database or external endpoint). Synthesize a typed target node from the
                    // span's own attributes instead of dropping the edge as "unknown". Unresolved
                    // (UnknownRemoteService) and raw-IP peers are suppressed here at the source.
                    remoteService = clientSpan.getDerivedRemoteService();
                    remoteOperation = clientSpan.getDerivedRemoteOperation() != null
                            ? clientSpan.getDerivedRemoteOperation()
                            : clientSpan.getOperationName();
                    remoteNodeType = clientSpan.getDerivedNodeType();
                    // Inherit the caller's environment so a prod dependency and a staging one with
                    // the same name stay distinct nodes rather than merging under generic:default.
                    // Brokers are the exception: they are shared by producers and consumers.
                    remoteEnvironment = NODE_TYPE_MESSAGING.equals(remoteNodeType)
                            ? MESSAGING_BROKER_ENVIRONMENT : clientSpan.getEnvironment();
                    remoteGroupByAttributes = Collections.emptyMap();
                    remoteDependencyAttributes = clientSpan.getDependencyAttributes();
                }

                final ClientSpanDecoration decoration = new ClientSpanDecoration(
                        null,
                        remoteEnvironment,
                        remoteService,
                        remoteOperation,
                        remoteGroupByAttributes,
                        remoteNodeType,
                        remoteDependencyAttributes
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
                                existingDecoration.getRemoteGroupByAttributes(),
                                existingDecoration.getRemoteNodeType(),
                                existingDecoration.getRemoteDependencyAttributes()
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
     * PHASE 2: Generate NodeOperationDetail events and metrics from ephemeral decorations.
     * Uses CLIENT-span-primary algorithm:
     *
     * Step 1 (CLIENT spans): Each CLIENT span in processingSpans emits a full NodeOperationDetail.
     * The CLIENT span's decoration contains all needed data: sourceNode from the span itself,
     * targetNode from remoteService/remoteEnvironment, sourceOperation from parentServerOperationName
     * (back-annotated in Phase 1 Pass 2), and targetOperation from remoteOperation.
     *
     * Step 2 (Leaf SERVER spans): SERVER spans with no CLIENT descendants emit a leaf
     * NodeOperationDetail (sourceNode + sourceOperation only, no target). Server metrics
     * are generated for ALL server spans regardless of leaf status.
     *
     * @param traceData Three-window trace data with ephemeral decorations (only processing spans are used)
     * @param currentTime Current timestamp
     * @param metricsStateByKey Shared map for metric aggregation across all traces
     * @return Collection of NodeOperationDetail events
     */
    private void generateNodeOperationDetailEvents(final ThreeWindowTraceDataWithDecorations traceData,
                                                    final Instant currentTime,
                                                    final Map<MetricKey, MetricAggregationState> sumStateByKey,
                                                    final Map<MetricKey, MetricAggregationState> histogramStateByKey,
                                                    final Set<NodeOperationDetail> dedupedNodeDetails,
                                                    final DependencyCardinalityLimiter dependencyLimiter) {
        // Step 1: CLIENT spans — primary emission path
        for (SpanStateData clientSpan : traceData.getProcessingSpans()) {
            if (SPAN_KIND_CLIENT.equals(clientSpan.getSpanKind())) {
                final ClientSpanDecoration serviceDecoration = traceData.getDecorations().getClientDecoration(clientSpan.getSpanId());

                if (serviceDecoration != null && !"unknown".equals(serviceDecoration.getRemoteService())) {
                    final ClientSpanDecoration decoration = NODE_TYPE_SERVICE.equals(serviceDecoration.getRemoteNodeType())
                            ? serviceDecoration
                            : limitDependencyDecoration(clientSpan, serviceDecoration, traceData, dependencyLimiter);
                    final Node sourceNode = new Node(
                            NODE_TYPE_SERVICE,
                            new Node.KeyAttributes(clientSpan.getEnvironment(), clientSpan.getServiceName()),
                            clientSpan.getGroupByAttributes()
                    );

                    final Node targetNode = new Node(
                            decoration.getRemoteNodeType() != null ? decoration.getRemoteNodeType() : NODE_TYPE_SERVICE,
                            new Node.KeyAttributes(decoration.getRemoteEnvironment(), decoration.getRemoteService()),
                            decoration.getRemoteGroupByAttributes(),
                            decoration.getRemoteDependencyAttributes()
                    );

                    final Operation sourceOp = decoration.getParentServerOperationName() != null
                            ? new Operation(decoration.getParentServerOperationName())
                            : null;
                    final Operation targetOp = new Operation(decoration.getRemoteOperation());

                    final Instant anchor = getAnchorTimestampFromSpan(clientSpan, currentTime,
                            metricTimestampGranularity.getChronoUnit());

                    final NodeOperationDetail nodeOperationDetail = new NodeOperationDetail(
                            sourceNode, targetNode, sourceOp, targetOp, anchor);

                    dedupedNodeDetails.add(nodeOperationDetail);

                    // Dependency decorations always carry a source operation (see limitDependencyDecoration),
                    // so their topology edge and metrics use the same operation.
                    if (decoration.getParentServerOperationName() != null) {
                        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                                clientSpan, decoration, currentTime, sumStateByKey, histogramStateByKey,
                                anchor, hostId);
                    }
                }
            }
        }

        // Step 2: SERVER spans — metrics for all, leaf NodeOperationDetail for those with no CLIENT descendants
        for (SpanStateData serverSpan : traceData.getProcessingSpans()) {
            if (SPAN_KIND_SERVER.equals(serverSpan.getSpanKind())) {
                final Instant anchor = getAnchorTimestampFromSpan(serverSpan, currentTime,
                        metricTimestampGranularity.getChronoUnit());
                ApmServiceMapMetricsUtil.generateMetricsForServerSpan(
                        serverSpan, currentTime, sumStateByKey, histogramStateByKey,
                        anchor, hostId);

                final ServerSpanDecoration decoration = traceData.getDecorations().getServerDecoration(serverSpan.getSpanId());

                if (decoration == null || decoration.getClientDescendants().isEmpty()) {
                    final Node sourceNode = new Node(
                            NODE_TYPE_SERVICE,
                            new Node.KeyAttributes(serverSpan.getEnvironment(), serverSpan.getServiceName()),
                            serverSpan.getGroupByAttributes()
                    );

                    final Operation sourceOp = new Operation(serverSpan.getOperationName());

                    final NodeOperationDetail nodeOperationDetail = new NodeOperationDetail(
                            sourceNode, null, sourceOp, null, anchor);

                    dedupedNodeDetails.add(nodeOperationDetail);
                }
            }
        }

        // Step 3: PRODUCER / CONSUMER spans — connect services through a synthesized messaging
        // broker node so async (Kafka, RabbitMQ, SQS/SNS) relationships appear in the service map.
        // A PRODUCER emits service -> broker; a CONSUMER emits broker -> service. The broker node
        // (type "messaging", named "{system}:{destination}") is the shared link between them.
        if (!dependencyNodesEnabled) {
            return;
        }
        final Set<String> processedMessageKeys = collectProcessedMessageKeys(traceData);
        for (SpanStateData messagingSpan : traceData.getProcessingSpans()) {
            final String spanKind = messagingSpan.getSpanKind();
            final boolean isProducer = SPAN_KIND_PRODUCER.equals(spanKind);
            final boolean isConsumer = SPAN_KIND_CONSUMER.equals(spanKind);
            // Require both a system and a destination, and pass the same noise filter as the CLIENT
            // path, so a missing destination never mints a "{system}:unknown" node.
            if ((isProducer || isConsumer)
                    && messagingSpan.getMessagingSystem() != null
                    && messagingSpan.getMessagingDestination() != null) {
                // "{system}:{destination}", derived once in SpanStateData so CLIENT-kind messaging
                // calls resolve to the same broker node.
                if (!isPublishableDependencyName(messagingSpan.getDerivedRemoteService())
                        || (isConsumer && isRedundantConsumer(messagingSpan, traceData, processedMessageKeys))) {
                    continue;
                }

                final String sourceKey = cardinalitySourceKey(messagingSpan);
                final String brokerName = dependencyLimiter.limitDependency(sourceKey, messagingSpan.getDerivedRemoteService());
                final String remoteOperationName = dependencyLimiter.limitRemoteOperation(sourceKey, brokerName,
                        messagingSpan.getMessagingOperation() != null
                                ? messagingSpan.getMessagingOperation()
                                : messagingSpan.getOperationName());

                final Node serviceNode = new Node(
                        NODE_TYPE_SERVICE,
                        new Node.KeyAttributes(messagingSpan.getEnvironment(), messagingSpan.getServiceName()),
                        messagingSpan.getGroupByAttributes()
                );
                final Node brokerNode = new Node(
                        NODE_TYPE_MESSAGING,
                        new Node.KeyAttributes(MESSAGING_BROKER_ENVIRONMENT, brokerName),
                        Collections.emptyMap(),
                        OTHER_REMOTE_SERVICE.equals(brokerName)
                                ? Collections.emptyMap() : messagingSpan.getDependencyAttributes()
                );

                // A producer publishes on behalf of the operation that is handling the request, as the
                // CLIENT path does; a consumer's own span is the operation that handles the message.
                final String serviceOperationName = isProducer
                        ? findEntryOperationName(messagingSpan, traceData)
                        : messagingSpan.getOperationName();
                final Operation serviceOp = new Operation(serviceOperationName);
                final Operation brokerOp = new Operation(remoteOperationName);

                final Instant anchor = getAnchorTimestampFromSpan(messagingSpan, currentTime,
                        metricTimestampGranularity.getChronoUnit());

                final NodeOperationDetail messagingDetail = isProducer
                        ? new NodeOperationDetail(serviceNode, brokerNode, serviceOp, brokerOp, anchor)
                        : new NodeOperationDetail(brokerNode, serviceNode, brokerOp, serviceOp, anchor);

                dedupedNodeDetails.add(messagingDetail);

                // RED metrics for the messaging edge, labeled by the broker (remoteService =
                // "{system}:{destination}"), reusing the CLIENT-span metric path. For a CONSUMER the
                // series is still keyed service=<consumer>, remoteService=<broker> (see README).
                final ClientSpanDecoration messagingDecoration = new ClientSpanDecoration(
                        serviceOperationName,
                        MESSAGING_BROKER_ENVIRONMENT,
                        brokerName,
                        remoteOperationName,
                        messagingSpan.getGroupByAttributes(),
                        NODE_TYPE_MESSAGING,
                        Collections.emptyMap());
                ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                        messagingSpan, messagingDecoration, currentTime, sumStateByKey,
                        histogramStateByKey, anchor, hostId);
            }
        }
    }

    /**
     * Apply the per-service cardinality caps to a synthesized dependency decoration and give it a source
     * operation: the parent SERVER operation when known, otherwise the nearest entry operation.
     */
    private ClientSpanDecoration limitDependencyDecoration(final SpanStateData clientSpan,
                                                           final ClientSpanDecoration decoration,
                                                           final ThreeWindowTraceData traceData,
                                                           final DependencyCardinalityLimiter dependencyLimiter) {
        final String sourceKey = cardinalitySourceKey(clientSpan);
        final String remoteService = dependencyLimiter.limitDependency(sourceKey, decoration.getRemoteService());
        final String remoteOperation = dependencyLimiter.limitRemoteOperation(sourceKey, remoteService,
                decoration.getRemoteOperation());
        final String sourceOperationName = decoration.getParentServerOperationName() != null
                ? decoration.getParentServerOperationName()
                : findEntryOperationName(clientSpan, traceData);
        return new ClientSpanDecoration(
                sourceOperationName,
                decoration.getRemoteEnvironment(),
                remoteService,
                remoteOperation,
                decoration.getRemoteGroupByAttributes(),
                decoration.getRemoteNodeType(),
                OTHER_REMOTE_SERVICE.equals(remoteService)
                        ? Collections.emptyMap() : decoration.getRemoteDependencyAttributes());
    }

    private static String cardinalitySourceKey(final SpanStateData span) {
        return span.getEnvironment() + "\u0000" + span.getServiceName();
    }

    /**
     * Whether a CLIENT span without a child SERVER span should become a synthesized dependency target.
     */
    private boolean isSynthesizedDependencyTarget(final SpanStateData clientSpan,
                                                  final ThreeWindowTraceData traceData,
                                                  final Set<String> knownServerServices) {
        if (!isDependencyCandidate(clientSpan)) {
            return false;
        }
        // Nested CLIENT spans in one service (an SDK span over its HTTP transport span) describe one call;
        // only the outermost is synthesized, so the call is not counted twice.
        final SpanStateData parent = clientSpan.getParentSpanId() != null
                ? traceData.getSpansBySpanId().get(clientSpan.getParentSpanId()) : null;
        if (parent != null && clientSpan.getServiceName().equals(parent.getServiceName())
                && (SPAN_KIND_CLIENT.equals(parent.getSpanKind()) && isDependencyCandidate(parent)
                    || isMessagingTransportParent(parent))) {
            return false;
        }
        // An inner CLIENT span of the same call reached a traced service, which already gets the edge.
        if (hasClientDescendantWithServerChild(clientSpan, traceData)) {
            return false;
        }
        // A peer named like an instrumented service is that service with its SERVER span missing from
        // this trace (late arrival, sampling), not an external dependency.
        return !(NODE_TYPE_EXTERNAL.equals(clientSpan.getDerivedNodeType())
                && namesKnownServerService(clientSpan, knownServerServices));
    }

    // A PRODUCER span, or a CONSUMER span that only receives, is the messaging call itself; a CLIENT span
    // under it is the transport of that call (e.g. the HTTP request of an SNS publish or SQS receive).
    // A CONSUMER span that processes a message is business logic, so CLIENT calls under it are kept.
    private static boolean isMessagingTransportParent(final SpanStateData parent) {
        if (parent.getMessagingSystem() == null) {
            return false;
        }
        return SPAN_KIND_PRODUCER.equals(parent.getSpanKind())
                || (SPAN_KIND_CONSUMER.equals(parent.getSpanKind()) && !isProcessOperation(parent));
    }

    private static boolean isDependencyCandidate(final SpanStateData span) {
        return span.getDerivedNodeType() != null && isPublishableDependencyName(span.getDerivedRemoteService());
    }

    private boolean hasClientDescendantWithServerChild(final SpanStateData clientSpan, final ThreeWindowTraceData traceData) {
        final Set<String> visited = new HashSet<>();
        final java.util.Deque<SpanStateData> pending = new java.util.ArrayDeque<>();
        pending.push(clientSpan);
        visited.add(clientSpan.getSpanId());
        while (!pending.isEmpty()) {
            final SpanStateData current = pending.pop();
            for (SpanStateData child : traceData.getChildrenByParentId().getOrDefault(current.getSpanId(), Collections.emptyList())) {
                if (current != clientSpan && SPAN_KIND_SERVER.equals(child.getSpanKind())) {
                    return true;
                }
                if (SPAN_KIND_CLIENT.equals(child.getSpanKind())
                        && clientSpan.getServiceName().equals(child.getServiceName())
                        && visited.add(child.getSpanId())) {
                    pending.push(child);
                }
            }
        }
        return false;
    }

    private static boolean namesKnownServerService(final SpanStateData clientSpan, final Set<String> knownServerServices) {
        if (knownServerServices.isEmpty()) {
            return false;
        }
        final Map<String, String> dependencyAttributes = clientSpan.getDependencyAttributes();
        for (final String candidate : new String[]{
                dependencyAttributes.get("peer.service"),
                dependencyAttributes.get("server.address"),
                SpanStateData.hostWithoutPort(clientSpan.getDerivedRemoteService())}) {
            if (candidate == null) {
                continue;
            }
            final String name = candidate.toLowerCase(Locale.ROOT);
            if (knownServerServices.contains(name)) {
                return true;
            }
            // Kubernetes service DNS: "checkout.shop.svc.cluster.local" is the "checkout" service.
            final int firstDot = name.indexOf('.');
            if (firstDot > 0 && (name.endsWith(".svc") || name.contains(".svc."))
                    && knownServerServices.contains(name.substring(0, firstDot))) {
                return true;
            }
        }
        return false;
    }

    private static Set<String> collectServerServiceNames(final Map<String, Collection<SpanStateData>> previousSpansByTraceId,
                                                         final Map<String, Collection<SpanStateData>> currentSpansByTraceId,
                                                         final Map<String, Collection<SpanStateData>> nextSpansByTraceId) {
        final Set<String> serviceNames = new HashSet<>();
        for (final Map<String, Collection<SpanStateData>> window : List.of(
                previousSpansByTraceId, currentSpansByTraceId, nextSpansByTraceId)) {
            for (final Collection<SpanStateData> spans : window.values()) {
                for (final SpanStateData span : spans) {
                    if (SPAN_KIND_SERVER.equals(span.getSpanKind()) && span.getServiceName() != null) {
                        serviceNames.add(span.getServiceName().toLowerCase(Locale.ROOT));
                    }
                }
            }
        }
        return serviceNames;
    }

    /**
     * The operation a span runs under: the nearest SERVER or CONSUMER ancestor in the same service,
     * or the span's own operation when it has none.
     */
    private String findEntryOperationName(final SpanStateData span, final ThreeWindowTraceData traceData) {
        final Set<String> visited = new HashSet<>();
        SpanStateData current = span;
        while (current.getParentSpanId() != null && visited.add(current.getSpanId())) {
            final SpanStateData parent = traceData.getSpansBySpanId().get(current.getParentSpanId());
            if (parent == null || !span.getServiceName().equals(parent.getServiceName())) {
                break;
            }
            if (SPAN_KIND_SERVER.equals(parent.getSpanKind()) || SPAN_KIND_CONSUMER.equals(parent.getSpanKind())) {
                return parent.getOperationName();
            }
            current = parent;
        }
        return span.getOperationName();
    }

    /**
     * One message can produce several CONSUMER spans in one service (receive + process, or a process
     * span nested under a receive span). Only one of them is counted: a nested CONSUMER span for the same
     * destination is skipped, and a non-process span is skipped when a process span exists for the same
     * message, i.e. the same service, destination and parent (producer) span.
     */
    private boolean isRedundantConsumer(final SpanStateData consumerSpan, final ThreeWindowTraceData traceData,
                                        final Set<String> processedMessageKeys) {
        if (isNestedConsumer(consumerSpan, traceData)) {
            return true;
        }
        return !isProcessOperation(consumerSpan) && consumerSpan.getParentSpanId() != null
                && processedMessageKeys.contains(messageKey(consumerSpan));
    }

    private static Set<String> collectProcessedMessageKeys(final ThreeWindowTraceData traceData) {
        final Set<String> keys = new HashSet<>();
        for (SpanStateData span : traceData.getLookupSpans()) {
            if (SPAN_KIND_CONSUMER.equals(span.getSpanKind()) && isProcessOperation(span)
                    && span.getParentSpanId() != null && !isNestedConsumer(span, traceData)) {
                keys.add(messageKey(span));
            }
        }
        return keys;
    }

    private static String messageKey(final SpanStateData consumerSpan) {
        return consumerSpan.getServiceName() + "\u0000" + consumerSpan.getDerivedRemoteService()
                + "\u0000" + consumerSpan.getParentSpanId();
    }

    private static boolean isNestedConsumer(final SpanStateData consumerSpan, final ThreeWindowTraceData traceData) {
        final SpanStateData parent = consumerSpan.getParentSpanId() != null
                ? traceData.getSpansBySpanId().get(consumerSpan.getParentSpanId()) : null;
        return parent != null
                && SPAN_KIND_CONSUMER.equals(parent.getSpanKind())
                && consumerSpan.getServiceName().equals(parent.getServiceName())
                && Objects.equals(consumerSpan.getDerivedRemoteService(), parent.getDerivedRemoteService());
    }

    private static boolean isProcessOperation(final SpanStateData span) {
        return MESSAGING_OPERATION_PROCESS.equalsIgnoreCase(span.getMessagingOperation());
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
