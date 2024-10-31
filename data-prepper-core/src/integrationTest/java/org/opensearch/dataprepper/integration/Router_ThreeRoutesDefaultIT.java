/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.test.framework.InMemorySinkAccessor;
import org.opensearch.dataprepper.plugins.test.framework.InMemorySourceAccessor;
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

class Router_ThreeRoutesDefaultIT {
    private static final String TESTING_KEY = "ConditionalRoutingIT";
    private static final String BASE_PATH = "src/integrationTest/resources/org/opensearch/dataprepper/";
    private static final String DATA_PREPPER_CONFIG_FILE = BASE_PATH + "configuration/data-prepper-config.yaml";
    private static final String PIPELINE_BASE_PATH = BASE_PATH + "pipeline/route/";
    private static final String PIPELINE_CONFIGURATION_UNDER_TEST = PIPELINE_BASE_PATH + "three-route-with-default-route.yaml";
    private static final String ALL_SOURCE_KEY = TESTING_KEY + "_all";
    private static final String ALPHA_SOURCE_KEY = TESTING_KEY + "_alpha";
    private static final String BETA_SOURCE_KEY = TESTING_KEY + "_beta";
    private static final String ALPHA_DEFAULT_SOURCE_KEY = TESTING_KEY + "_alpha_default";
    private static final String ALPHA_BETA_GAMMA_SOURCE_KEY = TESTING_KEY + "_alpha_beta_gamma";
    private static final String DEFAULT_SOURCE_KEY = TESTING_KEY + "_default";
    private static final String KNOWN_CONDITIONAL_KEY = "value";
    private static final String ALPHA_VALUE = "a";
    private static final String BETA_VALUE = "b";
    private static final String GAMMA_VALUE = "g";
    private static final String DEFAULT_VALUE = "z";
    private DataPrepperTestRunner dataPrepperTestRunner;
    private InMemorySourceAccessor inMemorySourceAccessor;
    private InMemorySinkAccessor inMemorySinkAccessor;

    @BeforeEach
    void setUp() {
        dataPrepperTestRunner = DataPrepperTestRunner.builder()
                .withPipelinesDirectoryOrFile(PIPELINE_CONFIGURATION_UNDER_TEST)
                .withDataPrepperConfigFile(DATA_PREPPER_CONFIG_FILE)
                .build();

        dataPrepperTestRunner.start();
        inMemorySourceAccessor = dataPrepperTestRunner.getInMemorySourceAccessor();
        inMemorySinkAccessor = dataPrepperTestRunner.getInMemorySinkAccessor();
    }

    @AfterEach
    void tearDown() {
        dataPrepperTestRunner.stop();
    }

    @Test
    void test_default_route() {
        final List<Record<Event>> alphaEvents = createEvents(ALPHA_VALUE, 10);
        final List<Record<Event>> betaEvents = createEvents(BETA_VALUE, 20);
        final List<Record<Event>> gammaEvents = createEvents(GAMMA_VALUE, 20);
        final List<Record<Event>> defaultEvents = createEvents(DEFAULT_VALUE, 20);

        final List<Record<Event>> allEvents = new ArrayList<>(alphaEvents);
        allEvents.addAll(betaEvents);
        allEvents.addAll(gammaEvents);
        allEvents.addAll(defaultEvents);
        Collections.shuffle(allEvents);

        inMemorySourceAccessor.submit(TESTING_KEY, allEvents);

        await().atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(inMemorySinkAccessor.get(ALPHA_SOURCE_KEY), not(empty()));
                    assertThat(inMemorySinkAccessor.get(BETA_SOURCE_KEY), not(empty()));
                    assertThat(inMemorySinkAccessor.get(ALL_SOURCE_KEY), not(empty()));
                    assertThat(inMemorySinkAccessor.get(ALPHA_DEFAULT_SOURCE_KEY), not(empty()));
                    assertThat(inMemorySinkAccessor.get(ALPHA_BETA_GAMMA_SOURCE_KEY), not(empty()));
                    assertThat(inMemorySinkAccessor.get(DEFAULT_SOURCE_KEY), not(empty()));
                });

        final List<Record<Event>> actualAlphaRecords = inMemorySinkAccessor.get(ALPHA_SOURCE_KEY);

        assertThat(actualAlphaRecords.size(), equalTo(alphaEvents.size()));

        assertThat(actualAlphaRecords, containsInAnyOrder(allEvents.stream()
                .filter(alphaEvents::contains).toArray()));

        final List<Record<Event>> actualBetaRecords = inMemorySinkAccessor.get(BETA_SOURCE_KEY);

        assertThat(actualBetaRecords.size(), equalTo(betaEvents.size()));

        assertThat(actualBetaRecords, containsInAnyOrder(allEvents.stream()
                .filter(betaEvents::contains).toArray()));

        final List<Record<Event>> actualDefaultRecords = inMemorySinkAccessor.get(DEFAULT_SOURCE_KEY);

        assertThat(actualDefaultRecords.size(), equalTo(defaultEvents.size()));
        assertThat(actualDefaultRecords, containsInAnyOrder(allEvents.stream()
                .filter(defaultEvents::contains).toArray()));

        final List<Record<Event>> actualAlphaDefaultRecords = new ArrayList<>();
        actualAlphaDefaultRecords.addAll(actualAlphaRecords);
        actualAlphaDefaultRecords.addAll(actualDefaultRecords);
        assertThat(actualAlphaDefaultRecords.size(), equalTo(defaultEvents.size()+alphaEvents.size()));
        assertThat(actualAlphaDefaultRecords, containsInAnyOrder(allEvents.stream()
                .filter(event -> defaultEvents.contains(event) || alphaEvents.contains(event)).toArray()));

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

