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
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.InMemorySinkAccessor;
import org.opensearch.dataprepper.plugins.InMemorySourceAccessor;
import org.opensearch.dataprepper.test.framework.DataPrepperTestRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

class ForwardingHeadlessPipelinesIT {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessorPipelineIT.class);
    private static final String IN_MEMORY_IDENTIFIER_DLQ = "PipelineDLQIT";
    private static final String IN_MEMORY_IDENTIFIER_FORWARD = "ForwardPipelineIT";
    private static final String PIPELINE_DLQ_TEST_CONFIGURATION= "pipeline-dlq.yaml";
    private static final String FORWARD_PIPELINE_TEST_CONFIGURATION= "forward-pipeline.yaml";
    private DataPrepperTestRunner dataPrepperTestRunner;
    private InMemorySourceAccessor inMemorySourceAccessor;
    private InMemorySinkAccessor inMemorySinkAccessor;

    private void createPipeline(final String pipelineConfiguration) {
        dataPrepperTestRunner = DataPrepperTestRunner.builder()
                .withPipelinesDirectoryOrFile(pipelineConfiguration)
                .build();
        inMemorySourceAccessor = dataPrepperTestRunner.getInMemorySourceAccessor();
        inMemorySinkAccessor = dataPrepperTestRunner.getInMemorySinkAccessor();
        dataPrepperTestRunner.start();
        LOG.info("Started test runner.");
    }

    @AfterEach
    void tearDown() {
        LOG.info("Test tear down. Stop the test runner.");
        dataPrepperTestRunner.stop();
    }

    @Test
    void pipeline_forward_test() {
        createPipeline(FORWARD_PIPELINE_TEST_CONFIGURATION);

        final int recordsToCreate = 200;
        final List<Record<Event>> inputRecords = IntStream.range(0, recordsToCreate)
                .mapToObj(i -> UUID.randomUUID().toString())
                .map(JacksonEvent::fromMessage)
                .map(Record::new)
                .collect(Collectors.toList());

        LOG.info("Submitting a batch of record.");
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER_FORWARD, inputRecords);

        await().atMost(400, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER_FORWARD), not(empty()));
        });

        assertThat(inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER_FORWARD).size(), equalTo(2*recordsToCreate));

        final List<Record<Event>> sinkRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER_FORWARD);

        for (int i = 0; i < sinkRecords.size(); i++) {
            final Record<Event> inputRecord = inputRecords.get(i%recordsToCreate);
            final Record<Event> sinkRecord = sinkRecords.get(i);
            assertThat(sinkRecord, notNullValue());
            final Event recordData = sinkRecord.getData();
            assertThat(recordData, notNullValue());
            assertThat(
                    recordData.get("message", String.class),
                    equalTo(inputRecord.getData().get("message", String.class)));
            assertThat(recordData.get("test1", String.class),
                    equalTo("knownUpdatedPrefix1" + i%recordsToCreate));

        }
    }
}
