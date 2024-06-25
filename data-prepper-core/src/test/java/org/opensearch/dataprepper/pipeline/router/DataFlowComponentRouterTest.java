/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.router;

import org.opensearch.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.parser.DataFlowComponent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;

@ExtendWith(MockitoExtension.class)
class DataFlowComponentRouterTest {

    @Mock
    private DataFlowComponent<TestComponent> dataFlowComponent;
    @Mock
    private TestComponent testComponent;
    @Mock
    private BiConsumer<TestComponent, Collection<Record>> componentRecordsConsumer;
    @Mock
    private RouterGetRecordStrategy getRecordStrategy;

    private Collection<Record> recordsIn;

    private static class TestComponent {
    }

    @BeforeEach
    void setUp() {
        recordsIn = Collections.emptyList();
        lenient().when(dataFlowComponent.getComponent()).thenReturn(testComponent);
        getRecordStrategy = mock(RouterGetRecordStrategy.class);
        lenient().when(getRecordStrategy.getRecord(any())).thenAnswer(a -> a.getArgument(0));
        lenient().when(getRecordStrategy.getAllRecords(anyCollection())).thenAnswer(a -> a.getArgument(0));
    }

    private DataFlowComponentRouter createObjectUnderTest() {
        return new DataFlowComponentRouter();
    }

    @Test
    void route_with_null_strategy_throws() {
        getRecordStrategy = null;
        assertThrows(NullPointerException.class, () -> createObjectUnderTest().route(recordsIn, dataFlowComponent, null, getRecordStrategy, componentRecordsConsumer));
    }

    @Test
    void route_with_null_strategy_throws_no_matching_routes() {
        getRecordStrategy = null;
        final Map<Record, Set<String>> noMatchingRoutes = recordsIn.stream()
                .collect(Collectors.toMap(Function.identity(), r -> Collections.emptySet()));
        assertThrows(NullPointerException.class, () -> createObjectUnderTest().route(recordsIn, dataFlowComponent, noMatchingRoutes, getRecordStrategy, componentRecordsConsumer));
    }

    @Nested
    class ComponentWithNoRoutes {

        @BeforeEach
        void setUp() {
            when(dataFlowComponent.getRoutes()).thenReturn(Collections.emptySet());

            recordsIn = IntStream.range(0, 10)
                    .mapToObj(i -> mock(Record.class))
                    .collect(Collectors.toList());
        }

        @Test
        void route_all_Events_when_none_have_routes() {
            final Map<Record, Set<String>> noMatchingRoutes = recordsIn.stream()
                    .collect(Collectors.toMap(Function.identity(), r -> Collections.emptySet()));

            createObjectUnderTest().route(recordsIn, dataFlowComponent, noMatchingRoutes, getRecordStrategy, componentRecordsConsumer);

            verify(componentRecordsConsumer).accept(testComponent, recordsIn);
        }

        @Test
        void route_all_Events_when_all_have_routes() {
            final Map<Record, Set<String>> allWithRoutes = recordsIn.stream()
                    .collect(Collectors.toMap(Function.identity(), r -> Collections.singleton(UUID.randomUUID().toString())));

            createObjectUnderTest().route(recordsIn, dataFlowComponent, allWithRoutes, getRecordStrategy, componentRecordsConsumer);

            verify(componentRecordsConsumer).accept(testComponent, recordsIn);
        }

        @Test
        void route_when_no_records() {
            recordsIn = Collections.emptyList();

            final Map<Record, Set<String>> noMatchingRoutes = Collections.emptyMap();

            createObjectUnderTest().route(recordsIn, dataFlowComponent, noMatchingRoutes, getRecordStrategy, componentRecordsConsumer);

            verify(componentRecordsConsumer).accept(testComponent, recordsIn);
        }

    }

    @Nested
    class ComponentWithSingleRoute {

        private String knownRoute;

        @BeforeEach
        void setUp() {
            knownRoute = UUID.randomUUID().toString();
            when(dataFlowComponent.getRoutes()).thenReturn(Collections.singleton(knownRoute));

            recordsIn = IntStream.range(0, 10)
                    .mapToObj(i -> mock(Record.class))
                    .collect(Collectors.toList());
        }

        @Test
        void route_no_Events_when_none_have_routes() {
            final Map<Record, Set<String>> noMatchingRoutes = recordsIn.stream()
                    .collect(Collectors.toMap(Function.identity(), r -> Collections.emptySet()));

            createObjectUnderTest().route(recordsIn, dataFlowComponent, noMatchingRoutes, getRecordStrategy, componentRecordsConsumer);

            verify(componentRecordsConsumer).accept(testComponent, Collections.emptyList());
        }

        @Test
        void route_no_Events_when_none_have_matching_routes() {
            final Map<Record, Set<String>> noMatchingRoutes = recordsIn.stream()
                    .collect(Collectors.toMap(Function.identity(), r -> Collections.singleton(UUID.randomUUID().toString())));

            createObjectUnderTest().route(recordsIn, dataFlowComponent, noMatchingRoutes, getRecordStrategy, componentRecordsConsumer);

            verify(componentRecordsConsumer).accept(testComponent, Collections.emptyList());
        }

        @Test
        void route_no_Events_when_none_have_matching_routes_with_default_route() {
            when(dataFlowComponent.getRoutes()).thenReturn(Set.of(DataFlowComponentRouter.DEFAULT_ROUTE));
            final Map<Record, Set<String>> noMatchingRoutes = recordsIn.stream()
                    .collect(Collectors.toMap(Function.identity(), r -> Collections.emptySet()));

            createObjectUnderTest().route(recordsIn, dataFlowComponent, noMatchingRoutes, getRecordStrategy, componentRecordsConsumer);

            verify(componentRecordsConsumer).accept(testComponent, recordsIn);
        }


        @Test
        void route_all_Events_when_all_have_matched_route() {
            final Map<Record, Set<String>> allMatchingRoutes = recordsIn.stream()
                    .collect(Collectors.toMap(Function.identity(), r -> Collections.singleton(knownRoute)));

            createObjectUnderTest().route(recordsIn, dataFlowComponent, allMatchingRoutes, getRecordStrategy, componentRecordsConsumer);

            verify(componentRecordsConsumer).accept(testComponent, recordsIn);
        }

        @Test
        void route_matching_Events_when_some_have_matched_route() {
            final Map<Record, Set<String>> someMatchingRoutes = new HashMap<>();
            boolean applyRoute = false;
            Collection<Record> expectedRecords = new ArrayList<>();
            for (Record record : recordsIn) {
                someMatchingRoutes.put(record, applyRoute ? Set.of(knownRoute, UUID.randomUUID().toString()) : Set.of(UUID.randomUUID().toString()) );
                if(applyRoute)
                    expectedRecords.add(record);

                applyRoute = !applyRoute;
            }

            createObjectUnderTest().route(recordsIn, dataFlowComponent, someMatchingRoutes, getRecordStrategy, componentRecordsConsumer);

            verify(componentRecordsConsumer).accept(testComponent, expectedRecords);
        }

        @Test
        void route_when_no_records() {
            recordsIn = Collections.emptyList();

            final Map<Record, Set<String>> noMatchingRoutes = Collections.emptyMap();

            createObjectUnderTest().route(recordsIn, dataFlowComponent, noMatchingRoutes, getRecordStrategy, componentRecordsConsumer);

            verify(componentRecordsConsumer).accept(testComponent, recordsIn);
        }

    }

    @Nested
    class ComponentWithMultipleRoute {

        private String knownRoute;

        @BeforeEach
        void setUp() {
            knownRoute = UUID.randomUUID().toString();
            when(dataFlowComponent.getComponent()).thenReturn(testComponent);
            when(dataFlowComponent.getRoutes()).thenReturn(Set.of(UUID.randomUUID().toString(), knownRoute, UUID.randomUUID().toString()));

            recordsIn = IntStream.range(0, 10)
                    .mapToObj(i -> mock(Record.class))
                    .collect(Collectors.toList());
        }

        @Test
        void route_no_Events_when_none_have_routes() {
            final Map<Record, Set<String>> noMatchingRoutes = recordsIn.stream()
                    .collect(Collectors.toMap(Function.identity(), r -> Collections.emptySet()));

            createObjectUnderTest().route(recordsIn, dataFlowComponent, noMatchingRoutes, getRecordStrategy, componentRecordsConsumer);

            verify(componentRecordsConsumer).accept(testComponent, Collections.emptyList());
        }

        @Test
        void route_no_Events_when_none_have_matching_routes() {
            final Map<Record, Set<String>> noMatchingRoutes = recordsIn.stream()
                    .collect(Collectors.toMap(Function.identity(), r -> Collections.singleton(UUID.randomUUID().toString())));

            createObjectUnderTest().route(recordsIn, dataFlowComponent, noMatchingRoutes, getRecordStrategy, componentRecordsConsumer);

            verify(componentRecordsConsumer).accept(testComponent, Collections.emptyList());
        }

        @Test
        void route_no_Events_when_none_have_matching_routes_with_default_route() {
            when(dataFlowComponent.getRoutes()).thenReturn(Set.of(DataFlowComponentRouter.DEFAULT_ROUTE));
            final Map<Record, Set<String>> noMatchingRoutes = recordsIn.stream()
                    .collect(Collectors.toMap(Function.identity(), r -> Collections.emptySet()));

            createObjectUnderTest().route(recordsIn, dataFlowComponent, noMatchingRoutes, getRecordStrategy, componentRecordsConsumer);

            verify(componentRecordsConsumer).accept(testComponent, recordsIn);
        }


        @Test
        void route_all_Events_when_all_have_matched_route() {
            final Map<Record, Set<String>> allMatchingRoutes = recordsIn.stream()
                    .collect(Collectors.toMap(Function.identity(), r -> Collections.singleton(knownRoute)));

            createObjectUnderTest().route(recordsIn, dataFlowComponent, allMatchingRoutes, getRecordStrategy, componentRecordsConsumer);

            verify(componentRecordsConsumer).accept(testComponent, recordsIn);
        }

        @Test
        void route_matching_Events_when_some_have_matched_route() {
            final Map<Record, Set<String>> someMatchingRoutes = new HashMap<>();
            boolean applyRoute = false;
            Collection<Record> expectedRecords = new ArrayList<>();
            for (Record record : recordsIn) {
                someMatchingRoutes.put(record, applyRoute ? Set.of(knownRoute, UUID.randomUUID().toString()) : Set.of(UUID.randomUUID().toString()) );
                if(applyRoute)
                    expectedRecords.add(record);

                applyRoute = !applyRoute;
            }

            createObjectUnderTest().route(recordsIn, dataFlowComponent, someMatchingRoutes, getRecordStrategy, componentRecordsConsumer);

            verify(componentRecordsConsumer).accept(testComponent, expectedRecords);
        }

        @Test
        void route_when_no_records() {
            recordsIn = Collections.emptyList();

            final Map<Record, Set<String>> noMatchingRoutes = Collections.emptyMap();

            createObjectUnderTest().route(recordsIn, dataFlowComponent, noMatchingRoutes, getRecordStrategy, componentRecordsConsumer);

            verify(componentRecordsConsumer).accept(testComponent, recordsIn);
        }

    }

}
