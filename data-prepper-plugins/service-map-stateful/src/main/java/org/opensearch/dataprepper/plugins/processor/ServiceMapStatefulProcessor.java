/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import org.apache.commons.codec.DecoderException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.SingleThread;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.common.primitives.SignedBytes;
import org.apache.commons.codec.binary.Hex;
import org.opensearch.dataprepper.plugins.processor.state.MapDbProcessorState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

@SingleThread
@DataPrepperPlugin(name = "service_map", deprecatedName = "service_map_stateful", pluginType = Processor.class,
        pluginConfigurationType = ServiceMapProcessorConfig.class)
public class ServiceMapStatefulProcessor extends AbstractProcessor<Record<Event>, Record<Event>> implements RequiresPeerForwarding {

    static final String SPANS_DB_SIZE = "spansDbSize";
    static final String TRACE_GROUP_DB_SIZE = "traceGroupDbSize";
    static final String SPANS_DB_COUNT = "spansDbCount";
    static final String TRACE_GROUP_DB_COUNT = "traceGroupDbCount";
    static final String RELATIONSHIP_COUNT = "relationshipCount";

    private static final Logger LOG = LoggerFactory.getLogger(ServiceMapStatefulProcessor.class);
    private static final String EMPTY_SUFFIX = "-empty";
    private static final String EVENT_TYPE = "event";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Collection<Record<Event>> EMPTY_COLLECTION = Collections.emptySet();
    private static final Integer TO_MILLIS = 1_000;

    // TODO: This should not be tracked in this class, move it up to the creator
    private static final AtomicInteger processorsCreated = new AtomicInteger(0);
    private static long previousTimestamp;
    private static long windowDurationMillis;
    private static CyclicBarrier allThreadsCyclicBarrier;

    private static volatile MapDbProcessorState<ServiceMapStateData> previousWindow;
    private static volatile MapDbProcessorState<ServiceMapStateData> currentWindow;
    private static volatile MapDbProcessorState<String> previousTraceGroupWindow;
    private static volatile MapDbProcessorState<String> currentTraceGroupWindow;
    private static volatile Set<ServiceNodeData> previousIsolatedServiceNodes;
    private static volatile Set<ServiceNodeData> currentIsolatedServiceNodes;
    //TODO: Consider keeping this state in a db
    private static final Set<ServiceMapRelationship> RELATIONSHIP_STATE = Sets.newConcurrentHashSet();
    private static File dbPath;
    private static Clock clock;

    private final int thisProcessorId;

    @DataPrepperPluginConstructor
    public ServiceMapStatefulProcessor(
            final ServiceMapProcessorConfig serviceMapProcessorConfig,
            final PluginMetrics pluginMetrics,
            final PipelineDescription pipelineDescription) {
        this((long) serviceMapProcessorConfig.getWindowDuration() * TO_MILLIS,
                new File(ServiceMapProcessorConfig.DEFAULT_DB_PATH),
                Clock.systemUTC(),
                pipelineDescription.getNumberOfProcessWorkers(),
                pluginMetrics);
    }

    ServiceMapStatefulProcessor(final long windowDurationMillis,
                                       final File databasePath,
                                       final Clock clock,
                                       final int processWorkers,
                                       final PluginMetrics pluginMetrics) {
        super(pluginMetrics);

        ServiceMapStatefulProcessor.clock = clock;
        this.thisProcessorId = processorsCreated.getAndIncrement();

        if (isMasterInstance()) {
            previousTimestamp = ServiceMapStatefulProcessor.clock.millis();
            ServiceMapStatefulProcessor.windowDurationMillis = windowDurationMillis;
            ServiceMapStatefulProcessor.dbPath = createPath(databasePath);

            currentWindow = new MapDbProcessorState<>(dbPath, getNewDbName(), processWorkers);
            previousWindow = new MapDbProcessorState<>(dbPath, getNewDbName() + EMPTY_SUFFIX, processWorkers);
            currentTraceGroupWindow = new MapDbProcessorState<>(dbPath, getNewTraceDbName(), processWorkers);
            previousTraceGroupWindow = new MapDbProcessorState<>(dbPath, getNewTraceDbName() + EMPTY_SUFFIX, processWorkers);
            currentIsolatedServiceNodes = Sets.newConcurrentHashSet();
            previousIsolatedServiceNodes = Sets.newConcurrentHashSet();

            allThreadsCyclicBarrier = new CyclicBarrier(processWorkers);
        }

        pluginMetrics.gauge(SPANS_DB_SIZE, this, serviceMapStateful -> serviceMapStateful.getSpansDbSize());
        pluginMetrics.gauge(TRACE_GROUP_DB_SIZE, this, serviceMapStateful -> serviceMapStateful.getTraceGroupDbSize());
        pluginMetrics.gauge(SPANS_DB_COUNT, this, serviceMapStateful -> serviceMapStateful.getSpansDbCount());
        pluginMetrics.gauge(TRACE_GROUP_DB_COUNT, this, serviceMapStateful -> serviceMapStateful.getTraceGroupDbCount());
        pluginMetrics.gauge(RELATIONSHIP_COUNT, this, serviceMapStateful -> serviceMapStateful.getRelationshipCount());
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

    /**
     * Adds the data for spans from the ResourceSpans object to the current window
     *
     * @param records Input records that will be modified/processed
     * @return If the window is reached, returns a list of ServiceMapRelationship objects representing the edges to be
     * added to the service map index. Otherwise, returns an empty set.
     */
    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        final Collection<Record<Event>> relationships = windowDurationHasPassed() ? evaluateEdges() : EMPTY_COLLECTION;
        final Map<byte[], ServiceMapStateData> batchStateData = new TreeMap<>(SignedBytes.lexicographicalComparator());
        records.forEach(i -> processSpan((Span) i.getData(), batchStateData));
        try {
            currentWindow.putAll(batchStateData);
        } catch (RuntimeException e) {
            LOG.error("Caught exception trying to put batch state data", e);
        }
        return relationships;
    }

    private void processSpan(final Span span, final Map<byte[], ServiceMapStateData> batchStateData) {
        if (span.getServiceName() != null) {
            final String serviceName = span.getServiceName();
            final String spanId = span.getSpanId();
            final String parentSpanId = span.getParentSpanId();
            final String spanKind = span.getKind();
            final byte[] traceId;
            try {
                traceId = Hex.decodeHex(span.getTraceId());
            } catch (DecoderException e) {
                LOG.error("Caught DecoderException when decoding the traceId.", e);
                return;
            }
            currentIsolatedServiceNodes.add(new ServiceNodeData(traceId, serviceName));
            try {
                batchStateData.put(
                        Hex.decodeHex(spanId),
                        new ServiceMapStateData(
                                serviceName,
                                parentSpanId.isEmpty()? null : Hex.decodeHex(parentSpanId),
                                traceId,
                                spanKind,
                                span.getName()));
            } catch (Exception e) {
                LOG.error("Caught exception trying to put service map state data into batch", e);
            }
            if (parentSpanId.isEmpty()) {
                try {
                    currentTraceGroupWindow.put(traceId, span.getName());
                } catch (Exception e) {
                    LOG.error("Caught exception trying to put trace group name", e);
                }
            }
        }
    }

    /**
     * This function parses the current and previous windows to find the edges, and rotates the window state objects.
     *
     * @return Set of Record<Event> containing json representation of ServiceMapRelationships found
     */
    private Collection<Record<Event>> evaluateEdges() {
        LOG.debug("Evaluating service map edges");
        try {
            final Collection<Record<Event>> serviceDependencyRecords = new HashSet<>();

            serviceDependencyRecords.addAll(iterateProcessorState(previousWindow));
            serviceDependencyRecords.addAll(iterateProcessorState(currentWindow));
            LOG.debug("Done evaluating service map edges");

            // Wait for all workers before rotating windows
            allThreadsCyclicBarrier.await();

            if (isMasterInstance()) {
                processIsolatedServiceMapNodes(serviceDependencyRecords);
                rotateWindows();
            }

            // Wait for all workers before exiting this method
            allThreadsCyclicBarrier.await();

            return serviceDependencyRecords;
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }

    private Collection<Record<Event>> iterateProcessorState(final MapDbProcessorState<ServiceMapStateData> processorState) {
        final Collection<Record<Event>> serviceDependencyRecords = new HashSet<>();

        if (processorState.getAll() != null && !processorState.getAll().isEmpty()) {
            processorState.getIterator(processorsCreated.get(), thisProcessorId).forEachRemaining(entry -> {
                final ServiceMapStateData child = entry.getValue();

                if (child.parentSpanId == null) {
                    return;
                }

                ServiceMapStateData parent = currentWindow.get(child.parentSpanId);
                if (parent == null) {
                    parent = previousWindow.get(child.parentSpanId);
                }

                final String traceGroupName = getTraceGroupName(child.traceId);
                if (parent == null || parent.serviceName.equals(child.serviceName)) {
                    return;
                }

                previousIsolatedServiceNodes.remove(new ServiceNodeData(parent.traceId, parent.serviceName));
                currentIsolatedServiceNodes.remove(new ServiceNodeData(parent.traceId, parent.serviceName));
                previousIsolatedServiceNodes.remove(new ServiceNodeData(parent.traceId, child.serviceName));
                currentIsolatedServiceNodes.remove(new ServiceNodeData(parent.traceId, child.serviceName));

                final ServiceMapRelationship destinationRelationship =
                        ServiceMapRelationship.newDestinationRelationship(parent.serviceName,
                                parent.spanKind, child.serviceName, child.name, traceGroupName);
                final ServiceMapRelationship targetRelationship = ServiceMapRelationship.newTargetRelationship(child.serviceName,
                        child.spanKind, child.serviceName, child.name, traceGroupName);


                // check if relationshipState has the above
                addServiceMapRelationship(serviceDependencyRecords, destinationRelationship);
                addServiceMapRelationship(serviceDependencyRecords, targetRelationship);
            });
        }

        return serviceDependencyRecords;
    }

    private void addServiceMapRelationship(
            final Collection<Record<Event>> serviceDependencyRecords, final ServiceMapRelationship serviceMapRelationship) {
        if (!RELATIONSHIP_STATE.contains(serviceMapRelationship)) {
            try {
                final Event destinationRelationshipEvent = JacksonEvent.builder()
                        .withEventType(EVENT_TYPE)
                        .withData(serviceMapRelationship)
                        .build();
                serviceDependencyRecords.add(new Record<>(destinationRelationshipEvent));
                RELATIONSHIP_STATE.add(serviceMapRelationship);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Checks both current and previous trace group windows for the trace id
     *
     * @param traceId
     * @return Trace group name for the given trace if it exists. Otherwise null.
     */
    private String getTraceGroupName(final byte[] traceId) {
        try {
            final String traceGroupName = currentTraceGroupWindow.get(traceId);
            return traceGroupName != null ? traceGroupName : previousTraceGroupWindow.get(traceId);
        } catch (RuntimeException e) {
            LOG.error("Caught exception trying to get trace group name", e);
            return null;
        }
    }


    @Override
    public void prepareForShutdown() {
        previousTimestamp = 0L;
    }

    @Override
    public boolean isReadyForShutdown() {
        return currentWindow.size() == 0;
    }

    @Override
    public void shutdown() {
        previousWindow.delete();
        currentWindow.delete();
        previousTraceGroupWindow.delete();
        currentTraceGroupWindow.delete();
        previousIsolatedServiceNodes.clear();
        currentIsolatedServiceNodes.clear();
    }

    private void processIsolatedServiceMapNodes(final Collection<Record<Event>> serviceDependencyRecords) {
        LOG.debug("Add isolated service nodes into service-map relationships.");
        previousIsolatedServiceNodes.forEach(serviceNodeData -> {
            final String traceGroupName;
            traceGroupName = getTraceGroupName(serviceNodeData.traceId);
            final ServiceMapRelationship serviceMapRelationship = ServiceMapRelationship.newIsolatedService(
                    serviceNodeData.serviceName, traceGroupName);
            addServiceMapRelationship(serviceDependencyRecords, serviceMapRelationship);
            LOG.debug("Added node {serviceName={}, traceGroupName={}} into service-map relationship.",
                    serviceNodeData.serviceName, traceGroupName);
        });
        LOG.debug("Done adding isolated service nodes");
    }

    /**
     * Rotate windows for processor state
     */
    private void rotateWindows() throws InterruptedException {
        LOG.debug("Rotating service map windows at " + clock.instant().toString());

        MapDbProcessorState tempWindow = previousWindow;
        previousWindow = currentWindow;
        currentWindow = tempWindow;
        currentWindow.clear();

        tempWindow = previousTraceGroupWindow;
        previousTraceGroupWindow = currentTraceGroupWindow;
        currentTraceGroupWindow = tempWindow;
        currentTraceGroupWindow.clear();

        Set<ServiceNodeData> tempNodesWindow = previousIsolatedServiceNodes;
        previousIsolatedServiceNodes = currentIsolatedServiceNodes;
        currentIsolatedServiceNodes = tempNodesWindow;
        currentIsolatedServiceNodes.clear();

        previousTimestamp = clock.millis();
        LOG.debug("Done rotating service map windows");
    }


    /**
     * @return Spans database size in bytes
     */
    public double getSpansDbSize() {
        return currentWindow.sizeInBytes() + previousWindow.sizeInBytes();
    }

    public double getSpansDbCount() {
        return currentWindow.size() + previousWindow.size();
    }

    /**
     * @return Trace group database size in bytes
     */
    public double getTraceGroupDbSize() {
        return currentTraceGroupWindow.sizeInBytes() + previousTraceGroupWindow.sizeInBytes();
    }
    public double getTraceGroupDbCount() {
        return currentTraceGroupWindow.size() + previousTraceGroupWindow.size();
    }
    public double getRelationshipCount() {
        return RELATIONSHIP_STATE.size();
    }

    /**
     * @return Next database name
     */
    private String getNewDbName() {
        return "db-" + clock.millis();
    }

    /**
     * @return Next database name
     */
    private String getNewTraceDbName() {
        return "trace-db-" + clock.millis();
    }

    /**
     * @return Boolean indicating whether the window duration has lapsed
     */
    private boolean windowDurationHasPassed() {
        if ((clock.millis() - previousTimestamp) >= windowDurationMillis) {
            return true;
        }
        return false;
    }

    /**
     * Master instance is needed to do things like window rotation that should only be done once
     *
     * @return Boolean indicating whether this object is the master ServiceMapStatefulProcessor instance
     */
    private boolean isMasterInstance() {
        return thisProcessorId == 0;
    }

    @Override
    public Collection<String> getIdentificationKeys() {
        return Collections.singleton("traceId");
    }

    private static class ServiceMapStateData implements Serializable {
        public String serviceName;
        public byte[] parentSpanId;
        public byte[] traceId;
        public String spanKind;
        public String name;

        public ServiceMapStateData() {
        }

        public ServiceMapStateData(final String serviceName, final byte[] parentSpanId,
                                   final byte[] traceId,
                                   final String spanKind,
                                   final String name) {
            this.serviceName = serviceName;
            this.parentSpanId = parentSpanId;
            this.traceId = traceId;
            this.spanKind = spanKind;
            this.name = name;
        }
    }

    private static class ServiceNodeData implements Serializable {
        public String serviceName;
        public byte[] traceId;

        public ServiceNodeData(final byte[] traceId, final String serviceName) {
            this.traceId = traceId;
            this.serviceName = serviceName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ServiceNodeData that = (ServiceNodeData) o;

            if (!Objects.equals(serviceName, that.serviceName)) return false;
            return Arrays.equals(traceId, that.traceId);
        }

        @Override
        public int hashCode() {
            int result = serviceName != null ? serviceName.hashCode() : 0;
            result = 31 * result + Arrays.hashCode(traceId);
            return result;
        }
    }
}