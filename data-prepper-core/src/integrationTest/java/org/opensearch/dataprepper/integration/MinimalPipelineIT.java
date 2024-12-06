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

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

class MinimalPipelineIT {

    private static final String IN_MEMORY_IDENTIFIER = "MinimalPipelineIT";
    private static final String BASE_PATH = "src/integrationTest/resources/org/opensearch/dataprepper/";
    private static final String DATA_PREPPER_CONFIG_FILE = BASE_PATH + "configuration/data-prepper-config.yaml";
    private static final String PIPELINE_BASE_PATH = BASE_PATH + "pipeline/";
    private static final String PIPELINE_CONFIGURATION_UNDER_TEST = PIPELINE_BASE_PATH + "minimal-pipeline.yaml";
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
    void pipeline_with_no_data() throws InterruptedException {

        final List<Record<Event>> preRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
        assertThat(preRecords, is(empty()));

        Thread.sleep(1000);

        final List<Record<Event>> postRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
        assertThat(postRecords, is(empty()));
    }

    @Test
    void pipeline_with_single_record() {

        final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
        final Record<Event> eventRecord = new Record<>(event);

        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, Collections.singletonList(eventRecord));

        await().atMost(400, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER), not(empty()));
        });

        assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER).size(), equalTo(1));
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

        await().atMost(400, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER), not(empty()));
        });

        assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER).size(), equalTo(recordsToCreate));
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

        await().atMost(400, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER), not(empty()));
        });

        assertThat(inMemorySinkAccessor.getAndClear(IN_MEMORY_IDENTIFIER).size(), equalTo(recordsToCreateBatch1));

        final int recordsToCreateBatch2 = 300;
        final List<Record<Event>> inputRecordsBatch2 = IntStream.range(0, recordsToCreateBatch2)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(JacksonEvent::fromMessage)
                .map(Record::new)
                .collect(Collectors.toList());

        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, inputRecordsBatch2);

        await().atMost(400, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER), not(empty()));
        });

        assertThat(inMemorySinkAccessor.getAndClear(IN_MEMORY_IDENTIFIER).size(), equalTo(recordsToCreateBatch2));
    }
}
