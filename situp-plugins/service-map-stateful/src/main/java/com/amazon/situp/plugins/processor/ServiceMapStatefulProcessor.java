package com.amazon.situp.plugins.processor;

import com.google.common.base.Charsets;
import com.amazon.situp.model.processor.Processor;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.plugins.processor.state.LmdbProcessorState;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.proto.trace.v1.ResourceSpans;

public class ServiceMapStatefulProcessor implements Processor<Record<ResourceSpans>, Record<String>> {
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

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Collection<Record<String>> EMPTY_COLLECTION = Collections.emptySet();

    private final int windowDuration;
    private LmdbProcessorState<ServiceMapStateData> previousWindow;
    private LmdbProcessorState<ServiceMapStateData> currentWindow;
    private long previousTimestamp;
    private File databasePath;
    private int dbNum = 0;

    public ServiceMapStatefulProcessor(final int windowDuration, final File databasePath) {
        this.databasePath = databasePath;
        this.windowDuration = windowDuration;
        this.currentWindow = new LmdbProcessorState<>(databasePath, getNewDbName(), ServiceMapStateData.class);
        this.previousWindow = new LmdbProcessorState<>(databasePath, getNewDbName(), ServiceMapStateData.class);
        previousTimestamp = System.currentTimeMillis();
    }

    private String getNewDbName() {
        dbNum++;
        return "db-" + dbNum;
    }

    private boolean windowDurationHasPassed() {
        if ((System.currentTimeMillis() - previousTimestamp) / 1000 >= windowDuration) {
            previousTimestamp = System.currentTimeMillis();
            return true;
        }
        return false;
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
                        new ServiceMapRelationship(parentStateData.serviceName, parentStateData.spanKind, serviceMapStateData.serviceName, null),
                        //This extra edge is added for compatibility of the index for both stateless and stateful processors
                        new ServiceMapRelationship(serviceMapStateData.serviceName, serviceMapStateData.spanKind, null, serviceMapStateData.serviceName)
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
                        new ServiceMapRelationship(parentStateData.serviceName, parentStateData.spanKind, serviceMapStateData.serviceName, null),
                        //This extra edge is added for compatibility of the index for both stateless and stateful processors
                        new ServiceMapRelationship(serviceMapStateData.serviceName, serviceMapStateData.spanKind, null, serviceMapStateData.serviceName)
                ).stream();
            } else {
                return null;
            }
        }
    };

    /**
     * This function parses the current and previous windows to find the edges, and rotates the window state objects.
     * @return
     */
    private Collection<Record<String>> findEdgesAndRotateWindows() {
        final Stream<ServiceMapRelationship> previousStream = previousWindow.iterate(previousWindowFunction).stream().flatMap(serviceMapEdgeStream -> serviceMapEdgeStream);
        final Stream<ServiceMapRelationship> currentStream = currentWindow.iterate(currentWindowFunction).stream().flatMap(serviceMapEdgeStream -> serviceMapEdgeStream);
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

        previousWindow.clear();
        previousWindow.close();
        previousWindow = currentWindow;
        currentWindow = new LmdbProcessorState(databasePath, getNewDbName(), ServiceMapStateData.class);
        return serviceDependencyRecords;
    }

    /**
     * Adds the data for spans from the ResourceSpans object to the current window
     * @param records Input records that will be modified/processed
     * @return If the window is reached, returns a list of ServiceMapRelationship objects representing the edges to be
     * added to the service map index. Otherwise, returns an empty set.
     */
    @Override
    public Collection<Record<String>> execute(Collection<Record<ResourceSpans>> records) {
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
        if (windowDurationHasPassed()) {
            return findEdgesAndRotateWindows();
        } else {
            return EMPTY_COLLECTION;
        }
    }

    public void deleteState() {
        previousWindow.clear();
        currentWindow.clear();
    }

    //TODO: Change to an override when a shutdown method is added to the processor interface
    public void shutdown() {
        previousWindow.close();
        currentWindow.close();
    }
}