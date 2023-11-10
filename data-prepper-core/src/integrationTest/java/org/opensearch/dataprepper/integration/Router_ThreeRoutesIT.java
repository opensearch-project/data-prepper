/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.InMemorySinkAccessor;
import org.opensearch.dataprepper.plugins.InMemorySourceAccessor;
import org.opensearch.dataprepper.test.framework.DataPrepperTestRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

class Router_ThreeRoutesIT {
    private static final String TESTING_KEY = "ConditionalRoutingIT";
    private static final String ALL_SOURCE_KEY = TESTING_KEY + "_all";
    private static final String ALPHA_SOURCE_KEY = TESTING_KEY + "_alpha";
    private static final String BETA_SOURCE_KEY = TESTING_KEY + "_beta";
    private static final String ALPHA_BETA_SOURCE_KEY = TESTING_KEY + "_alpha_beta";
    private static final String ALPHA_BETA_GAMMA_SOURCE_KEY = TESTING_KEY + "_alpha_beta_gamma";
    private static final String KNOWN_CONDITIONAL_KEY = "value";
    private static final String ALPHA_VALUE = "a";
    private static final String BETA_VALUE = "b";
    private DataPrepperTestRunner dataPrepperTestRunner;
    private InMemorySourceAccessor inMemorySourceAccessor;
    private InMemorySinkAccessor inMemorySinkAccessor;

    @BeforeEach
    void setUp() {
        dataPrepperTestRunner = DataPrepperTestRunner.builder()
                .withPipelinesDirectoryOrFile("route/three-route.yaml")
                .build();

        dataPrepperTestRunner.start();
        inMemorySourceAccessor = dataPrepperTestRunner.getInMemorySourceAccessor();
        inMemorySinkAccessor = dataPrepperTestRunner.getInMemorySinkAccessor();
    }

    @AfterEach
    void tearDown() {
        dataPrepperTestRunner.stop();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            ALL_SOURCE_KEY,
            ALPHA_BETA_SOURCE_KEY,
            ALPHA_BETA_GAMMA_SOURCE_KEY
    })
    void sending_alpha_and_beta_events_sends_to_sinks_that_take_both(final String sourceKeyToReceiveAll) {
        final List<Record<Event>> alphaEvents = createEvents(ALPHA_VALUE, 10);
        final List<Record<Event>> betaEvents = createEvents(BETA_VALUE, 20);

        final List<Record<Event>> allEvents = new ArrayList<>(alphaEvents);
        allEvents.addAll(betaEvents);
        Collections.shuffle(allEvents);

        inMemorySourceAccessor.submit(TESTING_KEY, allEvents);

        await().atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(inMemorySinkAccessor.get(sourceKeyToReceiveAll), not(empty()));
                });

        final List<Record<Event>> actualAllRecords = inMemorySinkAccessor.get(sourceKeyToReceiveAll);

        assertThat(actualAllRecords.size(), equalTo(allEvents.size()));
        assertThat(actualAllRecords, containsInAnyOrder(allEvents.toArray()));
    }

    @Test
    void sending_alpha_and_beta_events_sends_to_both_sinks() {
        final List<Record<Event>> alphaEvents = createEvents(ALPHA_VALUE, 10);
        final List<Record<Event>> betaEvents = createEvents(BETA_VALUE, 20);

        final List<Record<Event>> allEvents = new ArrayList<>(alphaEvents);
        allEvents.addAll(betaEvents);
        Collections.shuffle(allEvents);

        inMemorySourceAccessor.submit(TESTING_KEY, allEvents);

        await().atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(inMemorySinkAccessor.get(ALPHA_SOURCE_KEY), not(empty()));
                    assertThat(inMemorySinkAccessor.get(BETA_SOURCE_KEY), not(empty()));
                });

        final List<Record<Event>> actualAlphaRecords = inMemorySinkAccessor.get(ALPHA_SOURCE_KEY);

        assertThat(actualAlphaRecords.size(), equalTo(alphaEvents.size()));

        assertThat(actualAlphaRecords, containsInAnyOrder(allEvents.stream()
                .filter(alphaEvents::contains).toArray()));

        final List<Record<Event>> actualBetaRecords = inMemorySinkAccessor.get(BETA_SOURCE_KEY);

        assertThat(actualBetaRecords.size(), equalTo(betaEvents.size()));

        assertThat(actualBetaRecords, containsInAnyOrder(allEvents.stream()
                .filter(betaEvents::contains).toArray()));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            ALPHA_SOURCE_KEY,
            BETA_SOURCE_KEY,
            ALPHA_BETA_SOURCE_KEY,
            ALPHA_BETA_GAMMA_SOURCE_KEY
    })
    void sending_non_alpha_beta_events_never_reaches_sink(final String sourceKey) throws InterruptedException {
        final List<Record<Event>> randomEvents = createEvents(UUID.randomUUID().toString(), 20);

        Collections.shuffle(randomEvents);

        inMemorySourceAccessor.submit(TESTING_KEY, randomEvents);

        await().during(1200, TimeUnit.MILLISECONDS)
                .pollDelay(50, TimeUnit.MILLISECONDS)
                .until(() -> inMemorySinkAccessor.get(sourceKey), empty());
    }

    private List<Record<Event>> createEvents(final String value, final int numberToCreate) {
        return IntStream.range(0, numberToCreate)
                .mapToObj(i -> Map.of(KNOWN_CONDITIONAL_KEY, value, "arbitrary_field", UUID.randomUUID().toString()))
                .map(map -> JacksonEvent.builder().withData(map).withEventType("TEST").build())
                .map(jacksonEvent -> (Event) jacksonEvent)
                .map(Record::new)
                .collect(Collectors.toList());
    }
}
