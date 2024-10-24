/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.router;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.configuration.ConditionalRoute;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RouteEventEvaluatorTest {

    @Mock
    private ExpressionEvaluator evaluator;
    private List<ConditionalRoute> routes;

    private RouteEventEvaluator createObjectUnderTest() {
        return new RouteEventEvaluator(evaluator, routes);
    }

    @Nested
    class WithEmptyRoutes {
        @BeforeEach
        void setUp() {
            routes = Collections.emptyList();
        }

        @AfterEach
        void verifyNoEvaluation() {
            verifyNoInteractions(evaluator);
        }

        @Test
        void evaluateEventRoutes_with_empty_Records_returns_empty_map() {
            final Map<Record, Set<String>> recordsToRoutes = createObjectUnderTest().evaluateEventRoutes(Collections.emptyList());

            assertThat(recordsToRoutes, notNullValue());
            assertThat(recordsToRoutes, is(anEmptyMap()));
        }

        @Test
        void evaluateEventRoutes_with_Event_Records_returns_map_with_all_empty_routes() {
            final Collection<Record> records = createEventRecords();
            final Map<Record, Set<String>> recordsToRoutes = createObjectUnderTest().evaluateEventRoutes(records);

            assertThat(recordsToRoutes, notNullValue());

            assertThat(recordsToRoutes.size(), equalTo(records.size()));
            for (Record record : records) {
                assertThat(recordsToRoutes, hasKey(record));
            }

            for (Set<String> routes : recordsToRoutes.values()) {
                assertThat(routes, is(empty()));
            }
        }

        @Test
        void evaluateEventRoutes_with_non_Event_Records_returns_map_with_all_empty_routes() {
            final Collection<Record> records = createNonEventRecords();
            final Map<Record, Set<String>> recordsToRoutes = createObjectUnderTest().evaluateEventRoutes(records);

            assertThat(recordsToRoutes, notNullValue());

            assertThat(recordsToRoutes.size(), equalTo(records.size()));
            for (Record record : records) {
                assertThat(recordsToRoutes, hasKey(record));
            }

            for (Set<String> routes : recordsToRoutes.values()) {
                assertThat(routes, is(empty()));
            }
        }
    }

    @Nested
    @MockitoSettings(strictness = Strictness.LENIENT)
    class WithRoutes {
        private Set<String> allRouteNames;

        @BeforeEach
        void setUp() {
            routes = IntStream.range(0, 3)
                    .mapToObj(i -> mock(ConditionalRoute.class))
                    .peek(r -> when(r.getCondition()).thenReturn(UUID.randomUUID().toString()))
                    .peek(r -> when(r.getName()).thenReturn(UUID.randomUUID().toString()))
                    .collect(Collectors.toList());

            allRouteNames = routes
                    .stream()
                    .map(ConditionalRoute::getName)
                    .collect(Collectors.toSet());
        }

        @Test
        void evaluateEventRoutes_with_empty_Records_returns_empty_map() {
            final Map<Record, Set<String>> recordsToRoutes = createObjectUnderTest().evaluateEventRoutes(Collections.emptyList());

            assertThat(recordsToRoutes, notNullValue());
            assertThat(recordsToRoutes, is(anEmptyMap()));

            verifyNoInteractions(evaluator);
        }


        @Test
        void evaluateEventRoutes_with_non_Event_Records_returns_map_with_all_empty_routes() {
            final Collection<Record> records = createNonEventRecords();
            final Map<Record, Set<String>> recordsToRoutes = createObjectUnderTest().evaluateEventRoutes(records);

            assertThat(recordsToRoutes, notNullValue());

            assertThat(recordsToRoutes.size(), equalTo(records.size()));
            for (Record record : records) {
                assertThat(recordsToRoutes, hasKey(record));
            }

            for (Set<String> routes : recordsToRoutes.values()) {
                assertThat(routes, is(empty()));
            }

            verifyNoInteractions(evaluator);
        }

        @Test
        void evaluateEventRoutes_with_Event_Records_returns_map_with_matching_routes() {
            final List<Record> records = createEventRecords();

            final Record recordMatchingAllRoutes = records.get(1);
            final Event eventMatchingAllRoutes = (Event) records.get(1).getData();
            for (ConditionalRoute route : routes) {
                when(evaluator.evaluateConditional(route.getCondition(), eventMatchingAllRoutes))
                        .thenReturn(true);

                for (Record record : records) {
                    if(recordMatchingAllRoutes == record)
                        continue;

                    when(evaluator.evaluate(route.getCondition(), (Event) record.getData()))
                            .thenReturn(false);
                }
            }

            final Map<Record, Set<String>> recordsToRoutes = createObjectUnderTest().evaluateEventRoutes(records);

            assertThat(recordsToRoutes, notNullValue());

            assertThat(recordsToRoutes.size(), equalTo(records.size()));
            for (Record record : records) {
                assertThat(recordsToRoutes, hasKey(record));
            }

            final Set<String> actualRoutes = recordsToRoutes.get(recordMatchingAllRoutes);
            assertThat(actualRoutes.size(), equalTo(routes.size()));
            assertThat(actualRoutes, equalTo(allRouteNames));

            for (Map.Entry<Record, Set<String>> recordSetEntry : recordsToRoutes.entrySet()) {
                if(recordSetEntry.getKey() == recordMatchingAllRoutes)
                    continue;

                assertThat(recordSetEntry.getValue(), is(empty()));
            }
        }

        @Test
        void evaluateEventRoutes_with_Event_Records_returns_map_with_matching_routes_excludes_exceptions() {
            final List<Record> records = createEventRecords();

            final Record recordMatchingAllRoutes = records.get(1);
            final Event eventMatchingAllRoutes = (Event) records.get(1).getData();
            for (ConditionalRoute route : routes) {
                when(evaluator.evaluateConditional(route.getCondition(), eventMatchingAllRoutes))
                        .thenReturn(true);

                for (Record record : records) {
                    if(recordMatchingAllRoutes == record)
                        continue;

                    when(evaluator.evaluateConditional(route.getCondition(), (Event) record.getData()))
                            .thenThrow(RuntimeException.class);
                }
            }

            final Map<Record, Set<String>> recordsToRoutes = createObjectUnderTest().evaluateEventRoutes(records);

            assertThat(recordsToRoutes, notNullValue());

            assertThat(recordsToRoutes.size(), equalTo(records.size()));
            for (Record record : records) {
                assertThat(recordsToRoutes, hasKey(record));
            }

            final Set<String> actualRoutes = recordsToRoutes.get(recordMatchingAllRoutes);
            assertThat(actualRoutes.size(), equalTo(routes.size()));
            assertThat(actualRoutes, equalTo(allRouteNames));

            for (Map.Entry<Record, Set<String>> recordSetEntry : recordsToRoutes.entrySet()) {
                if(recordSetEntry.getKey() == recordMatchingAllRoutes)
                    continue;

                assertThat(recordSetEntry.getValue(), is(empty()));
            }
        }

    }

    private List<Record> createEventRecords() {
        return createRecords(() -> mock(Event.class));
    }

    private List<Record> createNonEventRecords() {
        return createRecords(() -> UUID.randomUUID().toString());
    }

    private List<Record> createRecords(final Supplier<Object> dataSupplier) {
        return IntStream.range(0, 3)
                .mapToObj(i -> mock(Record.class))
                .peek(r -> when(r.getData()).thenReturn(dataSupplier.get()))
                .collect(Collectors.toList());
    }
}
