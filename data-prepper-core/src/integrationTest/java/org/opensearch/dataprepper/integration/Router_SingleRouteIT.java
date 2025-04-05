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

class Router_SingleRouteIT {
    private static final String TESTING_KEY = "ConditionalRoutingIT";
    private static final String BASE_PATH = "src/integrationTest/resources/org/opensearch/dataprepper/";
    private static final String DATA_PREPPER_CONFIG_FILE = BASE_PATH + "configuration/data-prepper-config.yaml";
    private static final String PIPELINE_BASE_PATH = BASE_PATH + "pipeline/route/";
    private static final String PIPELINE_CONFIGURATION_UNDER_TEST = PIPELINE_BASE_PATH + "single-route.yaml";
    private static final String ALPHA_SOURCE_KEY = TESTING_KEY + "_alpha";
    private static final String KNOWN_CONDITIONAL_KEY = "value";
    private static final String ALPHA_VALUE = "a";
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
    void sending_alpha_events_sends_to_the_sink_with_alpha_only_routes() {
        final List<Record<Event>> alphaEvents = createEvents(ALPHA_VALUE, 10);
        final List<Record<Event>> otherEvents = createEvents(UUID.randomUUID().toString(), 20);

        final List<Record<Event>> allEvents = new ArrayList<>(alphaEvents);
        allEvents.addAll(otherEvents);
        Collections.shuffle(allEvents);

        inMemorySourceAccessor.submit(TESTING_KEY, allEvents);

        await().atMost(400, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(inMemorySinkAccessor.get(ALPHA_SOURCE_KEY), not(empty()));
                });

        final List<Record<Event>> actualAllRecords = inMemorySinkAccessor.get(ALPHA_SOURCE_KEY);

        assertThat(actualAllRecords.size(), equalTo(alphaEvents.size()));

        assertThat(actualAllRecords, containsInAnyOrder(allEvents.stream()
                .filter(alphaEvents::contains).toArray()));
    }

    @Test
    void sending_non_alpha_events_never_reaches_sink() throws InterruptedException {
        final List<Record<Event>> randomEvents = createEvents(UUID.randomUUID().toString(), 20);

        Collections.shuffle(randomEvents);

        inMemorySourceAccessor.submit(TESTING_KEY, randomEvents);

        Thread.sleep(1000);

        assertThat(inMemorySinkAccessor.get(ALPHA_SOURCE_KEY), empty());
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
