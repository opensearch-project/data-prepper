/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.router;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.parser.DataFlowComponent;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RouterTest {

    @Mock
    private RouteEventEvaluator routeEventEvaluator;

    @Mock
    private DataFlowComponentRouter dataFlowComponentRouter;
    private Collection<DataFlowComponent<TestComponent>> dataFlowComponents;
    @Mock
    private BiConsumer<TestComponent, Collection<Record>> componentRecordsConsumer;
    @Mock
    private RouterGetRecordStrategy getRecordStrategy;

    private Consumer<Event> noRouteHandler;

    private Collection<Record> recordsIn;

    private static class TestComponent {
    }

    @BeforeEach
    void setUp() {
        recordsIn = Collections.emptyList();
        dataFlowComponents = Collections.emptyList();
        getRecordStrategy = mock(RouterGetRecordStrategy.class);
    }

    private Router createObjectUnderTest() {
        noRouteHandler = mock(Consumer.class);
        return new Router(routeEventEvaluator, dataFlowComponentRouter, noRouteHandler);
    }

    private Router createObjectUnderTestWithDataFlowComponentRouter() {
        noRouteHandler = mock(Consumer.class);
        return new Router(routeEventEvaluator, new DataFlowComponentRouter(), noRouteHandler);
    }

    @Test
    void constructor_throws_with_null_routeEventEvaluator() {
        routeEventEvaluator = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void route_throws_if_records_is_null() {
        final Router objectUnderTest = createObjectUnderTest();

        assertThrows(NullPointerException.class, () -> objectUnderTest.route(null, dataFlowComponents, getRecordStrategy, componentRecordsConsumer));
    }

    @Test
    void route_throws_if_dataFlowComponents_is_null() {
        final Router objectUnderTest = createObjectUnderTest();

        assertThrows(NullPointerException.class, () -> objectUnderTest.route(recordsIn, null, getRecordStrategy, componentRecordsConsumer));
    }

    @Test
    void route_throws_if_getRecordStrategy_is_null() {
        final Router objectUnderTest = createObjectUnderTest();

        assertThrows(NullPointerException.class, () -> objectUnderTest.route(recordsIn, null, null, componentRecordsConsumer));
    }

    @Test
    void route_throws_if_componentRecordsConsumer_is_null() {
        final Router objectUnderTest = createObjectUnderTest();

        assertThrows(NullPointerException.class, () -> objectUnderTest.route(recordsIn, dataFlowComponents, getRecordStrategy, null));
    }

    @Nested
    class WithEmptyRecords {

        private Map<Record, Set<String>> recordsToRoutes;

        @BeforeEach
        void setUp() {
            recordsIn = Collections.emptyList();
            dataFlowComponents = Collections.emptyList();

            recordsToRoutes = Collections.singletonMap(
                    mock(Record.class), Collections.singleton(UUID.randomUUID().toString())
            );
            when(routeEventEvaluator.evaluateEventRoutes(recordsIn)).thenReturn(recordsToRoutes);
        }

        @Test
        void route_with_empty_DataFlowComponent() {
            createObjectUnderTest().route(recordsIn, dataFlowComponents, getRecordStrategy, componentRecordsConsumer);
        }

        @Test
        void route_with_single_DataFlowComponent() {
            DataFlowComponent<TestComponent> dataFlowComponent = mock(DataFlowComponent.class);
            dataFlowComponents = Collections.singletonList(dataFlowComponent);

            createObjectUnderTest().route(recordsIn, dataFlowComponents, getRecordStrategy, componentRecordsConsumer);

            verify(dataFlowComponentRouter).route(eq(recordsIn), eq(dataFlowComponent), eq(recordsToRoutes), eq(getRecordStrategy), any(BiConsumer.class));
        }

        @Test
        void route_with_multiple_DataFlowComponents() {

            dataFlowComponents = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                final DataFlowComponent dataFlowComponent = mock(DataFlowComponent.class);
                dataFlowComponents.add(dataFlowComponent);
            }

            createObjectUnderTest().route(recordsIn, dataFlowComponents, getRecordStrategy, componentRecordsConsumer);

            for (DataFlowComponent<TestComponent> dataFlowComponent : dataFlowComponents) {
                verify(dataFlowComponentRouter).route(eq(recordsIn), eq(dataFlowComponent), eq(recordsToRoutes), eq(getRecordStrategy), any(BiConsumer.class));
            }
        }
    }

    @Nested
    class WithUnroutedRecords {
        @Test
        void route_with_multiple_DataFlowComponents_with_unrouted_events() {
            Event event1 = mock(Event.class);
            Event event2 = mock(Event.class);
            Event event3 = mock(Event.class);
            EventHandle eventHandle3 = mock(EventHandle.class);
            Record record1 = mock(Record.class);
            Record record2 = mock(Record.class);
            Record record3 = mock(Record.class);
            Record record4 = mock(Record.class);
            lenient().when(record3.getData()).thenReturn(event3);
            Object notAnEvent = mock(Object.class);
            lenient().when(record4.getData()).thenReturn(notAnEvent);
            List<Record> recordsIn = List.of(record1, record2, record3, record4);
            Map<Record, Set<String>> recordsToRoutes = new HashMap<>();
            recordsToRoutes.put(record1, Set.of(UUID.randomUUID().toString()));
            recordsToRoutes.put(record2, Set.of(UUID.randomUUID().toString()));
            recordsToRoutes.put(record3, Set.of());
            recordsToRoutes.put(record4, Set.of());
            when(routeEventEvaluator.evaluateEventRoutes(recordsIn)).thenReturn(recordsToRoutes);
            dataFlowComponents = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                final DataFlowComponent dataFlowComponent = mock(DataFlowComponent.class);
                dataFlowComponents.add(dataFlowComponent);
                final Set<String> routes = Set.of(UUID.randomUUID().toString());
                when(dataFlowComponent.getRoutes()).thenReturn(routes);
            }

            createObjectUnderTest().route(recordsIn, dataFlowComponents, getRecordStrategy, componentRecordsConsumer);

            for (DataFlowComponent<TestComponent> dataFlowComponent : dataFlowComponents) {
                verify(dataFlowComponentRouter).route(eq(recordsIn), eq(dataFlowComponent), eq(recordsToRoutes), eq(getRecordStrategy), any(BiConsumer.class));
            }
            // Verify noRouteHandler gets invoked only for record3 and not
            // for record4, because record4 has non-Event type data
            verify(noRouteHandler, times(1)).accept(any());
        }

        @Test
        void route_with_multiple_DataFlowComponents_with_unrouted_events_with_allrouted() {
            Event event1 = mock(Event.class);
            Event event2 = mock(Event.class);
            Event event3 = mock(Event.class);
            EventHandle eventHandle3 = mock(EventHandle.class);
            Record record1 = mock(Record.class);
            Record record2 = mock(Record.class);
            Record record3 = mock(Record.class);
            Record record4 = mock(Record.class);
            lenient().when(record3.getData()).thenReturn(event3);
            Object notAnEvent = mock(Object.class);
            lenient().when(record4.getData()).thenReturn(notAnEvent);
            List<Record> recordsIn = List.of(record1, record2, record3, record4);
            Map<Record, Set<String>> recordsToRoutes = new HashMap<>();
            recordsToRoutes.put(record1, Set.of(UUID.randomUUID().toString()));
            recordsToRoutes.put(record2, Set.of(UUID.randomUUID().toString()));
            recordsToRoutes.put(record3, Set.of());
            recordsToRoutes.put(record4, Set.of());
            when(routeEventEvaluator.evaluateEventRoutes(recordsIn)).thenReturn(recordsToRoutes);
            dataFlowComponents = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                final DataFlowComponent dataFlowComponent = mock(DataFlowComponent.class);
                dataFlowComponents.add(dataFlowComponent);
                final Set<String> routes = i ==0 ? Collections.emptySet() : Set.of(UUID.randomUUID().toString());
                lenient().when(dataFlowComponent.getRoutes()).thenReturn(routes);
            }

            createObjectUnderTest().route(recordsIn, dataFlowComponents, getRecordStrategy, componentRecordsConsumer);

            for (DataFlowComponent<TestComponent> dataFlowComponent : dataFlowComponents) {
                verify(dataFlowComponentRouter).route(eq(recordsIn), eq(dataFlowComponent), eq(recordsToRoutes), eq(getRecordStrategy), any(BiConsumer.class));
            }
            // Verify noRouteHandler gets invoked only for record3 and not
            // for record4, because record4 has non-Event type data
            verify(noRouteHandler, times(0)).accept(any());
        }

        @Test
        void route_with_multiple_DataFlowComponents_with_unrouted_events_and_sink_with_noroutes() {
            Event event1 = mock(Event.class);
            Event event2 = mock(Event.class);
            Event event3 = mock(Event.class);
            EventHandle eventHandle3 = mock(EventHandle.class);
            Record record1 = mock(Record.class);
            Record record2 = mock(Record.class);
            Record record3 = mock(Record.class);
            Record record4 = mock(Record.class);
            Object notAnEvent = mock(Object.class);
            List<Record> recordsIn = List.of(record1, record2, record3, record4);
            Map<Record, Set<String>> recordsToRoutes = new HashMap<>();
            recordsToRoutes.put(record1, Set.of(UUID.randomUUID().toString()));
            recordsToRoutes.put(record2, Set.of(UUID.randomUUID().toString()));
            recordsToRoutes.put(record3, Set.of());
            recordsToRoutes.put(record4, Set.of());
            when(routeEventEvaluator.evaluateEventRoutes(recordsIn)).thenReturn(recordsToRoutes);
            dataFlowComponents = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                final DataFlowComponent dataFlowComponent = mock(DataFlowComponent.class);
                dataFlowComponents.add(dataFlowComponent);
                final Set<String> routes = i ==0 ? Collections.emptySet() : Set.of(UUID.randomUUID().toString());
                when(dataFlowComponent.getRoutes()).thenReturn(routes);
            }
            when(getRecordStrategy.getAllRecords(any())).thenReturn(recordsIn);

            createObjectUnderTestWithDataFlowComponentRouter().route(recordsIn, dataFlowComponents, getRecordStrategy, componentRecordsConsumer);

            verify(noRouteHandler, times(0)).accept(any());
        }
    }

    @Nested
    class WithRecords {

        private Map<Record, Set<String>> recordsToRoutes;

        @BeforeEach
        void setUp() {
            recordsIn = IntStream.range(0, 10)
                    .mapToObj(i -> mock(Record.class))
                    .collect(Collectors.toList());
            ;
            dataFlowComponents = Collections.emptyList();

            recordsToRoutes = recordsIn
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), r -> Collections.singleton(UUID.randomUUID().toString())));
            when(routeEventEvaluator.evaluateEventRoutes(recordsIn)).thenReturn(recordsToRoutes);
        }

        @Test
        void route_with_single_DataFlowComponent() {
            DataFlowComponent<TestComponent> dataFlowComponent = mock(DataFlowComponent.class);
            dataFlowComponents = Collections.singletonList(dataFlowComponent);

            createObjectUnderTest().route(recordsIn, dataFlowComponents, getRecordStrategy, componentRecordsConsumer);

            verify(dataFlowComponentRouter).route(eq(recordsIn), eq(dataFlowComponent), eq(recordsToRoutes), eq(getRecordStrategy), any(BiConsumer.class));
        }

        @Test
        void route_with_multiple_DataFlowComponents() {

            dataFlowComponents = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                final DataFlowComponent dataFlowComponent = mock(DataFlowComponent.class);
                dataFlowComponents.add(dataFlowComponent);
            }

            createObjectUnderTest().route(recordsIn, dataFlowComponents, getRecordStrategy, componentRecordsConsumer);

            for (DataFlowComponent<TestComponent> dataFlowComponent : dataFlowComponents) {
                verify(dataFlowComponentRouter).route(eq(recordsIn), eq(dataFlowComponent), eq(recordsToRoutes), eq(getRecordStrategy), any(BiConsumer.class));
            }
        }


        @Test
        void route_with_multiple_DataFlowComponents_And_Strategy() {
            final DataFlowComponent dataFlowComponent = mock(DataFlowComponent.class);
            dataFlowComponents = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                dataFlowComponents.add(dataFlowComponent);
            }
            createObjectUnderTest().route(recordsIn, dataFlowComponents, getRecordStrategy, componentRecordsConsumer);
            verify(dataFlowComponentRouter, times(5)).route(eq(recordsIn), eq(dataFlowComponent), eq(recordsToRoutes), eq(getRecordStrategy), any(BiConsumer.class));
        }
    }
}
