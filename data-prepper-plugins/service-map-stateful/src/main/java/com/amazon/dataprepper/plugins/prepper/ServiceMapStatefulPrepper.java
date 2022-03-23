/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.SingleThread;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.prepper.AbstractPrepper;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.trace.Span;
import com.amazon.dataprepper.plugins.prepper.state.MapDbPrepperState;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.common.primitives.SignedBytes;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

@SingleThread
@DataPrepperPlugin(name = "service_map_stateful", pluginType = Prepper.class)
public class ServiceMapStatefulPrepper extends AbstractPrepper<Record<Object>, Record<Event>> {

    public static final String SPANS_DB_SIZE = "spansDbSize";
    public static final String TRACE_GROUP_DB_SIZE = "traceGroupDbSize";

    private static final Logger LOG = LoggerFactory.getLogger(ServiceMapStatefulPrepper.class);
    private static final String EMPTY_SUFFIX = "-empty";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Collection<Record<Event>> EMPTY_COLLECTION = Collections.emptySet();
    private static final Integer TO_MILLIS = 1_000;

    // TODO: This should not be tracked in this class, move it up to the creator
    private static final AtomicInteger preppersCreated = new AtomicInteger(0);
    private static long previousTimestamp;
    private static long windowDurationMillis;
    private static CyclicBarrier allThreadsCyclicBarrier;

    private static volatile MapDbPrepperState<ServiceMapStateData> previousWindow;
    private static volatile MapDbPrepperState<ServiceMapStateData> currentWindow;
    private static volatile MapDbPrepperState<String> previousTraceGroupWindow;
    private static volatile MapDbPrepperState<String> currentTraceGroupWindow;
    //TODO: Consider keeping this state in a db
    private static final Set<ServiceMapRelationship> RELATIONSHIP_STATE = Sets.newConcurrentHashSet();
    private static File dbPath;
    private static Clock clock;

    private final int thisPrepperId;

    public ServiceMapStatefulPrepper(final PluginSetting pluginSetting) {
        this(pluginSetting.getIntegerOrDefault(ServiceMapPrepperConfig.WINDOW_DURATION, ServiceMapPrepperConfig.DEFAULT_WINDOW_DURATION) * TO_MILLIS,
                new File(ServiceMapPrepperConfig.DEFAULT_DB_PATH),
                Clock.systemUTC(),
                pluginSetting.getNumberOfProcessWorkers(),
                pluginSetting);
    }

    public ServiceMapStatefulPrepper(final long windowDurationMillis,
                                     final File databasePath,
                                     final Clock clock,
                                     final int processWorkers,
                                     final PluginSetting pluginSetting) {
        super(pluginSetting);

        ServiceMapStatefulPrepper.clock = clock;
        this.thisPrepperId = preppersCreated.getAndIncrement();

        if (isMasterInstance()) {
            previousTimestamp = ServiceMapStatefulPrepper.clock.millis();
            ServiceMapStatefulPrepper.windowDurationMillis = windowDurationMillis;
            ServiceMapStatefulPrepper.dbPath = createPath(databasePath);

            currentWindow = new MapDbPrepperState<>(dbPath, getNewDbName(), processWorkers);
            previousWindow = new MapDbPrepperState<>(dbPath, getNewDbName() + EMPTY_SUFFIX, processWorkers);
            currentTraceGroupWindow = new MapDbPrepperState<>(dbPath, getNewTraceDbName(), processWorkers);
            previousTraceGroupWindow = new MapDbPrepperState<>(dbPath, getNewTraceDbName() + EMPTY_SUFFIX, processWorkers);

            allThreadsCyclicBarrier = new CyclicBarrier(processWorkers);
        }

        pluginMetrics.gauge(SPANS_DB_SIZE, this, serviceMapStateful -> serviceMapStateful.getSpansDbSize());
        pluginMetrics.gauge(TRACE_GROUP_DB_SIZE, this, serviceMapStateful -> serviceMapStateful.getTraceGroupDbSize());
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
    public Collection<Record<Event>> doExecute(Collection<Record<Object>> records) {
        final Collection<Record<Event>> relationships = windowDurationHasPassed() ? evaluateEdges() : EMPTY_COLLECTION;
        final Map<byte[], ServiceMapStateData> batchStateData = new TreeMap<>(SignedBytes.lexicographicalComparator());
        records.forEach(i -> {
            final Object recordData = i.getData();
            // TODO: remove support for ExportTraceServiceRequest in 2.0
            if (recordData instanceof ExportTraceServiceRequest) {
                processExportTraceServiceRequest((ExportTraceServiceRequest) recordData, batchStateData);
            } else if (recordData instanceof Span) {
                processSpan((Span) recordData, batchStateData);
            } else {
                throw new RuntimeException("Unsupported record data type: " + recordData.getClass());
            }
        });
        try {
            currentWindow.putAll(batchStateData);
        } catch (RuntimeException e) {
            LOG.error("Caught exception trying to put batch state data", e);
        }
        return relationships;
    }

    private void processExportTraceServiceRequest(
            final ExportTraceServiceRequest exportTraceServiceRequest, final Map<byte[], ServiceMapStateData> batchStateData) {
        exportTraceServiceRequest.getResourceSpansList().forEach(resourceSpans -> {
            OTelHelper.getServiceName(resourceSpans.getResource()).ifPresent(serviceName -> resourceSpans.getInstrumentationLibrarySpansList().forEach(
                    instrumentationLibrarySpans -> {
                        instrumentationLibrarySpans.getSpansList().forEach(
                                span -> {
                                    if (OTelHelper.checkValidSpan(span)) {
                                        try {
                                            batchStateData.put(
                                                    span.getSpanId().toByteArray(),
                                                    new ServiceMapStateData(
                                                            serviceName,
                                                            span.getParentSpanId().isEmpty() ? null : span.getParentSpanId().toByteArray(),
                                                            span.getTraceId().toByteArray(),
                                                            span.getKind().name(),
                                                            span.getName()));
                                        } catch (RuntimeException e) {
                                            LOG.error("Caught exception trying to put service map state data into batch", e);
                                        }
                                        if (span.getParentSpanId().isEmpty()) {
                                            try {
                                                currentTraceGroupWindow.put(span.getTraceId().toByteArray(), span.getName());
                                            } catch (RuntimeException e) {
                                                LOG.error("Caught exception trying to put trace group name", e);
                                            }
                                        }
                                    } else {
                                        LOG.warn("Invalid span received");
                                    }
                                });
                    }
            ));
        });
    }

    private void processSpan(final Span span, final Map<byte[], ServiceMapStateData> batchStateData) {
        if (span.getServiceName() != null) {
            final String serviceName = span.getServiceName();
            final String spanId = span.getSpanId();
            final String traceId = span.getTraceId();
            final String parentSpanId = span.getParentSpanId();
            try {
                batchStateData.put(
                        Hex.decodeHex(spanId),
                        new ServiceMapStateData(
                                serviceName,
                                parentSpanId.isEmpty()? null : Hex.decodeHex(parentSpanId),
                                Hex.decodeHex(traceId),
                                span.getKind(),
                                span.getName()));
            } catch (Exception e) {
                LOG.error("Caught exception trying to put service map state data into batch", e);
            }
            if (parentSpanId.isEmpty()) {
                try {
                    currentTraceGroupWindow.put(Hex.decodeHex(traceId), span.getName());
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
        LOG.info("Evaluating service map edges");
        try {
            final Collection<Record<Event>> serviceDependencyRecords = new HashSet<>();

            serviceDependencyRecords.addAll(iteratePrepperState(previousWindow));
            serviceDependencyRecords.addAll(iteratePrepperState(currentWindow));
            LOG.info("Done evaluating service map edges");

            // Wait for all workers before rotating windows
            allThreadsCyclicBarrier.await();

            if (isMasterInstance()) {
                rotateWindows();
            }

            // Wait for all workers before exiting this method
            allThreadsCyclicBarrier.await();

            return serviceDependencyRecords;
        } catch (InterruptedException | BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }

    private Collection<Record<Event>> iteratePrepperState(final MapDbPrepperState<ServiceMapStateData> prepperState) {
        final Collection<Record<Event>> serviceDependencyRecords = new HashSet<>();

        if (prepperState.getAll() != null && !prepperState.getAll().isEmpty()) {
            prepperState.getIterator(preppersCreated.get(), thisPrepperId).forEachRemaining(entry -> {
                final ServiceMapStateData child = entry.getValue();

                if (child.parentSpanId == null) {
                    return;
                }

                ServiceMapStateData parent = currentWindow.get(child.parentSpanId);
                if (parent == null) {
                    parent = previousWindow.get(child.parentSpanId);
                }


                final String traceGroupName = getTraceGroupName(child.traceId);
                if (traceGroupName == null || parent == null || parent.serviceName.equals(child.serviceName)) {
                    return;
                }

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
                        .withEventType("event")
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
    }

    // TODO: Temp code, complex instance creation logic should be moved to a separate class
    static void resetStaticCounters() {
        preppersCreated.set(0);
    }


    /**
     * Rotate windows for prepper state
     */
    private void rotateWindows() throws InterruptedException {
        LOG.info("Rotating service map windows at " + clock.instant().toString());

        MapDbPrepperState tempWindow = previousWindow;
        previousWindow = currentWindow;
        currentWindow = tempWindow;
        currentWindow.clear();

        tempWindow = previousTraceGroupWindow;
        previousTraceGroupWindow = currentTraceGroupWindow;
        currentTraceGroupWindow = tempWindow;
        currentTraceGroupWindow.clear();

        previousTimestamp = clock.millis();
        LOG.info("Done rotating service map windows");
    }


    /**
     * @return Spans database size in bytes
     */
    public double getSpansDbSize() {
        return currentWindow.sizeInBytes() + previousWindow.sizeInBytes();
    }

    /**
     * @return Trace group database size in bytes
     */
    public double getTraceGroupDbSize() {
        return currentTraceGroupWindow.sizeInBytes() + previousTraceGroupWindow.sizeInBytes();
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
     * @return Boolean indicating whether this object is the master ServiceMapStatefulPrepper instance
     */
    private boolean isMasterInstance() {
        return thisPrepperId == 0;
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
}