package com.amazon.situp.plugins.processor;

import com.google.common.base.Charsets;
import com.amazon.situp.model.processor.Processor;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.plugins.processor.state.LmdbProcessorState;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.proto.trace.v1.ResourceSpans;

public class ServiceMapStatefulProcessor implements Processor<Record<ResourceSpans>, Record<String>> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Collection<Record<String>> EMPTY_COLLECTION = Collections.emptySet();
    private static final String MIN_SPAN_ID = "+++++++++++=";
    private static final String MAX_SPAN_ID = "zzzzzzzzzzz=";
    private static final int SPAN_ID_LEN = 12;

    //Static fields that need to be shared between instances of the class
    private static int numProcessors;
    private static AtomicInteger processorsCreated = new AtomicInteger(0);
    private static long previousTimestamp;
    private static long windowDurationMillis;
    private static CountDownLatch edgeEvaluationLatch = new CountDownLatch(numProcessors);
    private static CountDownLatch windowRotationLatch = new CountDownLatch(1);
    private static final KeyRangeSplitter keyRangeSplitter =
            new KeyRangeSplitter(MIN_SPAN_ID, MAX_SPAN_ID, StandardCharsets.UTF_8, SPAN_ID_LEN);
    private volatile static LmdbProcessorState<ServiceMapStateData> previousWindow;
    private volatile static LmdbProcessorState<ServiceMapStateData> currentWindow;
    private static File databasePath;
    private static int dbNum = 0;
    private static Clock clock;

    private final int thisProcessorId;
    private String iterationStartIndex;
    private String iterationStopIndex;

    public ServiceMapStatefulProcessor(final long windowDurationMillis, final File databasePath, final int numProcessors,
                                       final Clock clock) {
        this.clock = clock;
        this.thisProcessorId = processorsCreated.getAndIncrement();
        if(isMasterInstance()) {
            //TODO: Read num processors from config when its available
            this.numProcessors = numProcessors;
            previousTimestamp = this.clock.millis();
            this.databasePath = databasePath;
            this.windowDurationMillis = windowDurationMillis;
            this.currentWindow = new LmdbProcessorState<>(databasePath, getNewDbName(), ServiceMapStateData.class);
            this.previousWindow = new LmdbProcessorState<>(databasePath, getNewDbName(), ServiceMapStateData.class);
        }
        iterationStartIndex = keyRangeSplitter.getBoundary(thisProcessorId, ServiceMapStatefulProcessor.numProcessors);
        iterationStopIndex = keyRangeSplitter.getBoundary(thisProcessorId +1, ServiceMapStatefulProcessor.numProcessors);
    }

    /**
     * Adds the data for spans from the ResourceSpans object to the current window
     * @param records Input records that will be modified/processed
     * @return If the window is reached, returns a list of ServiceMapRelationship objects representing the edges to be
     * added to the service map index. Otherwise, returns an empty set.
     */
    @Override
    public Collection<Record<String>> execute(Collection<Record<ResourceSpans>> records) {
        final Collection<Record<String>> relationships = windowDurationHasPassed() ? evaluateEdges() : EMPTY_COLLECTION;
        records.forEach(resourceSpansRecord -> {
            final String resourceName = resourceSpansRecord.getData().getResource().getAttributesList().stream().filter(
                    keyValue -> keyValue.getKey().equals(ServiceMapSpanTags.SERVICE_NAME_KEY)
            ).findFirst().get().getValue().getStringValue();
            resourceSpansRecord.getData().getInstrumentationLibrarySpansList().forEach(
                    instrumentationLibrarySpans -> instrumentationLibrarySpans.getSpansList().forEach(span ->
                            currentWindow.put(
                                    span.getSpanId().toString(Charsets.UTF_8),
                                    new ServiceMapStateData(
                                            resourceName,
                                            span.getParentSpanId().toString(Charsets.UTF_8),
                                            span.getKind().name())
                            )
                    )
            );
        });
        return relationships;
    }

    /**
     * This function parses the current and previous windows to find the edges, and rotates the window state objects.
     * @return Set of Record<String> containing json representation of ServiceMapRelationships found
     */
    private Collection<Record<String>> evaluateEdges() {
        try {
            final Stream<ServiceMapRelationship> previousStream = previousWindow.iterate(previousWindowFunction, iterationStartIndex, iterationStopIndex).stream().flatMap(serviceMapEdgeStream -> serviceMapEdgeStream);
            final Stream<ServiceMapRelationship> currentStream = currentWindow.iterate(currentWindowFunction, iterationStartIndex, iterationStopIndex).stream().flatMap(serviceMapEdgeStream -> serviceMapEdgeStream);
            final Collection<Record<String>> serviceDependencyRecords =
                    Stream.concat(previousStream, currentStream).filter(Objects::nonNull)
                            .map(serviceDependency -> {
                                try {
                                    return new Record<>(OBJECT_MAPPER.writeValueAsString(serviceDependency));
                                } catch (JsonProcessingException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                            .collect(Collectors.toSet());

            doneEvaluatingEdges();
            waitForEvaluationFinish();

            if(isMasterInstance()) {
                rotateWindows();
                resetWorkState();
            } else {
                waitForRotationFinish();
            }

            return serviceDependencyRecords;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This function is used to iterate over the previous window and find parent/child relationships with the current
     * window. It only needs to find parents that live in the current window (and not the previous window) because these
     * spans have already been checked against the "previous" window.
     */
    private final BiFunction<String, ServiceMapStateData, Stream<ServiceMapRelationship>> previousWindowFunction = new BiFunction<String, ServiceMapStateData, Stream<ServiceMapRelationship>>() {
        @Override
        public Stream<ServiceMapRelationship> apply(String s, ServiceMapStateData serviceMapStateData) {
            final ServiceMapStateData parentStateData = currentWindow.get(serviceMapStateData.parentSpanId);
            if (parentStateData != null && !parentStateData.serviceName.equals(serviceMapStateData.serviceName)) {
                return Arrays.asList(
                        ServiceMapRelationship.newDestinationRelationship(parentStateData.serviceName, parentStateData.spanKind, serviceMapStateData.serviceName),
                        //This extra edge is added for compatibility of the index for both stateless and stateful processors
                        ServiceMapRelationship.newTargetRelationship(serviceMapStateData.serviceName, serviceMapStateData.spanKind, serviceMapStateData.serviceName)
                ).stream();
            } else {
                return null;
            }
        }
    };

    /**
     * This function is used to iterate over the current window and find parent/child relationships in the current and
     * previous windows.
     */
    private final BiFunction<String, ServiceMapStateData, Stream<ServiceMapRelationship>> currentWindowFunction = new BiFunction<String, ServiceMapStateData, Stream<ServiceMapRelationship>>() {
        @Override
        public Stream<ServiceMapRelationship> apply(String s, ServiceMapStateData serviceMapStateData) {
            ServiceMapStateData parentStateData = previousWindow.get(serviceMapStateData.parentSpanId);
            if (parentStateData == null) {
                parentStateData = currentWindow.get(serviceMapStateData.parentSpanId);
            }
            if (parentStateData != null && !parentStateData.serviceName.equals(serviceMapStateData.serviceName)) {
                return Arrays.asList(
                        ServiceMapRelationship.newDestinationRelationship(parentStateData.serviceName, parentStateData.spanKind, serviceMapStateData.serviceName),
                        //This extra edge is added for compatibility of the index for both stateless and stateful processors
                        ServiceMapRelationship.newTargetRelationship(serviceMapStateData.serviceName, serviceMapStateData.spanKind, serviceMapStateData.serviceName)
                ).stream();
            } else {
                return null;
            }
        }
    };

    /**
     * Delete current state held in the processor
     */
    public void deleteState() {
        previousWindow.clear();
        currentWindow.clear();
    }

    //TODO: Change to an override when a shutdown method is added to the processor interface
    public void shutdown() {
        previousWindow.close();
        currentWindow.close();
    }

    /**
     * Indicate/notify that this instance has finished evaluating edges
     */
    private void doneEvaluatingEdges() {
        edgeEvaluationLatch.countDown();
    }

    /**
     * Wait on all instances to finish evaluating edges
     * @throws InterruptedException
     */
    private void waitForEvaluationFinish() throws InterruptedException {
        edgeEvaluationLatch.await();
    }

    /**
     * Indicate that window rotation is complete
     */
    private void doneRotatingWindows() {
        windowRotationLatch.countDown();
    }

    /**
     * Wait on window rotation to complete
     * @throws InterruptedException
     */
    private void waitForRotationFinish() throws InterruptedException {
        windowRotationLatch.await();
    }

    /**
     * Reset state that indicates whether edge evaluation and window rotation is complete
     */
    private void resetWorkState() {
        windowRotationLatch = new CountDownLatch(1);
        edgeEvaluationLatch = new CountDownLatch(numProcessors);
    }

    /**
     * Rotate windows for processor state
     */
    private void rotateWindows() {
        if(isMasterInstance()) {
            previousWindow.clear();
            previousWindow.close();
        }
        previousWindow = currentWindow;
        currentWindow = new LmdbProcessorState(databasePath, getNewDbName(), ServiceMapStateData.class);
        previousTimestamp = clock.millis();
        doneRotatingWindows();
    }

    /**
     * @return Next database name
     */
    private String getNewDbName() {
        dbNum++;
        return "db-" + dbNum;
    }

    /**
     * @return Boolean indicating whether the window duration has lapsed
     */
    private boolean windowDurationHasPassed() {
        if ((clock.millis() - previousTimestamp)  >= windowDurationMillis) {
            return true;
        }
        return false;
    }

    /**
     * Master instance is needed to do things like window rotation that should only be done once
     * @return Boolean indicating whether this object is the master ServiceMapStatefulProcessor instance
     */
    private boolean isMasterInstance() {
        return thisProcessorId == 0;
    }

    //TODO: Remove this once we have a common class for holding otel attribute tags
    public static class ServiceMapSpanTags {
        public static final String SERVICE_NAME_KEY = "resource.name";
    }

    private static class ServiceMapStateData {
        public String serviceName;
        public String parentSpanId;
        public String spanKind;

        public ServiceMapStateData() {
        }

        public ServiceMapStateData(final String serviceName, final String parentSpanId,
                                   final String spanKind) {
            this.serviceName = serviceName;
            this.parentSpanId = parentSpanId;
            this.spanKind = spanKind;
        }
    }
}