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
import org.opensearch.dataprepper.plugins.InMemorySinkAccessor;
import org.opensearch.dataprepper.plugins.InMemorySourceAccessor;
import org.opensearch.dataprepper.test.framework.DataPrepperTestRunner;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class Connected_SingleExtraSinkIT {
    private static final String IN_MEMORY_IDENTIFIER = "Connected_SingleExtraSinkIT";
    private static final String IN_MEMORY_IDENTIFIER_ENTRY_SINK = IN_MEMORY_IDENTIFIER + "_Entry";
    private static final String IN_MEMORY_IDENTIFIER_EXIT_SINK = IN_MEMORY_IDENTIFIER + "_Exit";
    private static final String PIPELINE_CONFIGURATION_UNDER_TEST = "connected/single-connection-extra-sink.yaml";
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
    void pipeline_with_single_batch_of_records() {
        final int recordsToCreate = 200;
        final List<Record<Event>> inputRecords = IntStream.range(0, recordsToCreate)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(JacksonEvent::fromMessage)
                .map(Record::new)
                .collect(Collectors.toList());

        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, inputRecords);

        await().atMost(800, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER_ENTRY_SINK).size(), equalTo(recordsToCreate));
                    assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER_EXIT_SINK).size(), equalTo(recordsToCreate));
                });

        assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER_ENTRY_SINK).size(), equalTo(recordsToCreate));
        assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER_EXIT_SINK).size(), equalTo(recordsToCreate));
    }

    @Test
    void pipeline_with_multiple_batches_of_records() {
        final int recordsToCreateBatch1 = 200;
        final List<Record<Event>> inputRecordsBatch1 = IntStream.range(0, recordsToCreateBatch1)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(JacksonEvent::fromMessage)
                .map(Record::new)
                .collect(Collectors.toList());

        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, inputRecordsBatch1);

        await().atMost(800, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER_ENTRY_SINK).size(), equalTo(recordsToCreateBatch1));
                    assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER_EXIT_SINK).size(), equalTo(recordsToCreateBatch1));
                });

        assertThat(inMemorySinkAccessor.getAndClear(IN_MEMORY_IDENTIFIER_ENTRY_SINK).size(), equalTo(recordsToCreateBatch1));
        assertThat(inMemorySinkAccessor.getAndClear(IN_MEMORY_IDENTIFIER_EXIT_SINK).size(), equalTo(recordsToCreateBatch1));

        final int recordsToCreateBatch2 = 300;
        final List<Record<Event>> inputRecordsBatch2 = IntStream.range(0, recordsToCreateBatch2)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(JacksonEvent::fromMessage)
                .map(Record::new)
                .collect(Collectors.toList());

        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, inputRecordsBatch2);

        await().atMost(400, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER_ENTRY_SINK).size(), equalTo(recordsToCreateBatch2));
                    assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER_EXIT_SINK).size(), equalTo(recordsToCreateBatch2));
                });

        assertThat(inMemorySinkAccessor.getAndClear(IN_MEMORY_IDENTIFIER_ENTRY_SINK).size(), equalTo(recordsToCreateBatch2));
        assertThat(inMemorySinkAccessor.getAndClear(IN_MEMORY_IDENTIFIER_EXIT_SINK).size(), equalTo(recordsToCreateBatch2));
    }

}
