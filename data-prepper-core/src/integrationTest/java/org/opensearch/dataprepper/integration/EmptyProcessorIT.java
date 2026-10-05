/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.EmptyProcessor;
import org.opensearch.dataprepper.plugins.InMemorySinkAccessor;
import org.opensearch.dataprepper.plugins.InMemorySourceAccessor;
import org.opensearch.dataprepper.test.framework.DataPrepperTestRunner;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

/**
 * Integration test verifying that a processor with no mandatory configuration
 * works correctly when configured as:
 * <ul>
 *   <li>bare key (empty): {@code empty_processor:}</li>
 *   <li>explicit null: {@code empty_processor: null}</li>
 *   <li>explicit empty string: {@code empty_processor: ''}</li>
 * </ul>
 *
 * The pipeline configures 3 instances of {@code empty_processor}. Each instance
 * increments a counter on every event it processes. After all processors run,
 * the counter should equal 3 — proving all three configuration forms produce
 * valid, functioning processor instances.
 *
 * @see EmptyProcessor
 */
class EmptyProcessorIT {
    private static final String IN_MEMORY_IDENTIFIER = "EmptyProcessorIT";
    private static final String PIPELINE_CONFIGURATION_UNDER_TEST = "empty-processor-pipeline.yaml";
    private DataPrepperTestRunner dataPrepperTestRunner;
    private InMemorySourceAccessor inMemorySourceAccessor;
    private InMemorySinkAccessor inMemorySinkAccessor;

    @BeforeEach
    void setUp() {
        dataPrepperTestRunner = DataPrepperTestRunner.builder()
                .withPipelinesDirectoryOrFile(PIPELINE_CONFIGURATION_UNDER_TEST)
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
    void pipeline_with_single_record_has_count_equal_to_3() {
        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        final Record<Event> eventRecord = new Record<>(event);

        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, Collections.singletonList(eventRecord));

        await().atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER), not(empty()));
                });

        final List<Record<Event>> records = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
        assertThat(records.size(), equalTo(1));

        final Integer count = records.get(0).getData().get(EmptyProcessor.COUNT_KEY, Integer.class);
        assertThat("All 3 empty_processor instances (bare key, null, empty string) should have processed the event",
                count, equalTo(3));
    }

    @Test
    void pipeline_with_single_batch_of_records() {
        final int recordsToCreate = 200;
        final List<Record<Event>> inputRecords = IntStream.range(0, recordsToCreate)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(JacksonEvent::fromMessage)
                .map(Record::new)
                .collect(Collectors.toList());

        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, inputRecords);

        await().atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER), not(empty()));
                });

        final List<Record<Event>> sinkRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
        assertThat(sinkRecords.size(), equalTo(recordsToCreate));

        for (final Record<Event> record : sinkRecords) {
            final Integer count = record.getData().get(EmptyProcessor.COUNT_KEY, Integer.class);
            assertThat("Each record should be processed by all 3 empty_processor instances",
                    count, equalTo(3));
        }
    }
}
