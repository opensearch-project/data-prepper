/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.test.framework.DataPrepperTestRunner;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.InMemorySinkAccessor;
import org.opensearch.dataprepper.plugins.InMemorySourceAccessor;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;

class PipelinesWithAcksIT {

    private static final String IN_MEMORY_IDENTIFIER = "PipelinesWithAcksIT";
    private static final String SIMPLE_PIPELINE_CONFIGURATION_UNDER_TEST = "acknowledgements/simple-test.yaml";
    private static final String TWO_PIPELINES_CONFIGURATION_UNDER_TEST = "acknowledgements/two-pipelines-test.yaml";
    private static final String TWO_PARALLEL_PIPELINES_CONFIGURATION_UNDER_TEST = "acknowledgements/two-parallel-pipelines-test.yaml";
    private static final String THREE_PIPELINES_CONFIGURATION_UNDER_TEST = "acknowledgements/three-pipelines-test.yaml";
    private static final String THREE_PIPELINES_WITH_ROUTE_CONFIGURATION_UNDER_TEST = "acknowledgements/three-pipeline-route-test.yaml";
    private static final String THREE_PIPELINES_MULTI_SINK_CONFIGURATION_UNDER_TEST = "acknowledgements/three-pipelines-test-multi-sink.yaml";
    private static final String ONE_PIPELINE_THREE_SINKS_CONFIGURATION_UNDER_TEST = "acknowledgements/one-pipeline-three-sinks.yaml";
    private static final String ONE_PIPELINE_ACK_EXPIRY_CONFIGURATION_UNDER_TEST = "acknowledgements/one-pipeline-ack-expiry-test.yaml";
    private DataPrepperTestRunner dataPrepperTestRunner;
    private InMemorySourceAccessor inMemorySourceAccessor;
    private InMemorySinkAccessor inMemorySinkAccessor;

    void setUp(String configFile) {
        dataPrepperTestRunner = DataPrepperTestRunner.builder()
                .withPipelinesDirectoryOrFile(configFile)
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
    void simple_pipeline_with_single_record() {
        setUp(SIMPLE_PIPELINE_CONFIGURATION_UNDER_TEST);
        final int numRecords = 1;
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, numRecords);

        await().atMost(2000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            List<Record<Event>> outputRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
            assertThat(outputRecords, not(empty()));
            assertThat(outputRecords.size(), equalTo(numRecords));
        });
        assertTrue(inMemorySourceAccessor.getAckReceived());

    }

    @Test
    void simple_pipeline_with_multiple_records() {
        setUp(SIMPLE_PIPELINE_CONFIGURATION_UNDER_TEST);
        final int numRecords = 100;
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, numRecords);

        await().atMost(2000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            List<Record<Event>> outputRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
            assertThat(outputRecords, not(empty()));
            assertThat(outputRecords.size(), equalTo(numRecords));
        });
        assertTrue(inMemorySourceAccessor.getAckReceived());
    }

    @Test
    void two_pipelines_with_multiple_records() {
        setUp(TWO_PIPELINES_CONFIGURATION_UNDER_TEST);
        final int numRecords = 100;
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, numRecords);

        await().atMost(5000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            List<Record<Event>> outputRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
            assertThat(outputRecords, not(empty()));
            assertThat(outputRecords.size(), equalTo(numRecords));
        });
        assertTrue(inMemorySourceAccessor.getAckReceived());
    }

    @Test
    void three_pipelines_with_multiple_records() {
        setUp(THREE_PIPELINES_CONFIGURATION_UNDER_TEST);
        final int numRecords = 100;
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, numRecords);

        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            List<Record<Event>> outputRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
            assertThat(outputRecords, not(empty()));
            assertThat(outputRecords.size(), equalTo(numRecords));
        });
        assertTrue(inMemorySourceAccessor.getAckReceived());
    }

    @Test
    void three_pipelines_with_route_and_multiple_records() {
        setUp(THREE_PIPELINES_WITH_ROUTE_CONFIGURATION_UNDER_TEST);
        final int numRecords = 100;
        inMemorySourceAccessor.submitWithStatus(IN_MEMORY_IDENTIFIER, numRecords);

        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            List<Record<Event>> outputRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
            assertThat(outputRecords, not(empty()));
            assertThat(outputRecords.size(), equalTo(numRecords));
        });
        assertTrue(inMemorySourceAccessor.getAckReceived());
    }

    @Test
    void two_parallel_pipelines_multiple_records() {
        setUp(TWO_PARALLEL_PIPELINES_CONFIGURATION_UNDER_TEST);
        final int numRecords = 100;
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, numRecords);

        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            List<Record<Event>> outputRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
            assertThat(outputRecords, not(empty()));
            assertThat(outputRecords.size(), equalTo(2*numRecords));
        });
        assertTrue(inMemorySourceAccessor.getAckReceived());
    }

    @Test
    void three_pipelines_multi_sink_multiple_records() {
        setUp(THREE_PIPELINES_MULTI_SINK_CONFIGURATION_UNDER_TEST);
        final int numRecords = 100;
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, numRecords);

        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            List<Record<Event>> outputRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
            assertThat(outputRecords, not(empty()));
            assertThat(outputRecords.size(), equalTo(3*numRecords));
        });
        assertTrue(inMemorySourceAccessor.getAckReceived());
    }

    @Test
    void one_pipeline_three_sinks_multiple_records() {
        setUp(ONE_PIPELINE_THREE_SINKS_CONFIGURATION_UNDER_TEST);
        final int numRecords = 100;
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, numRecords);

        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            List<Record<Event>> outputRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
            assertThat(outputRecords, not(empty()));
            assertThat(outputRecords.size(), equalTo(3*numRecords));
        });
        assertTrue(inMemorySourceAccessor.getAckReceived());
    }

    @Test
    void one_pipeline_ack_expiry_multiple_records() {
        setUp(ONE_PIPELINE_ACK_EXPIRY_CONFIGURATION_UNDER_TEST);
        final int numRecords = 100;
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, numRecords);

        await().atMost(20000, TimeUnit.MILLISECONDS)
          .untilAsserted(() -> {
            List<Record<Event>> outputRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
            assertThat(outputRecords, not(empty()));
            assertThat(outputRecords.size(), equalTo(numRecords));
        });
        assertThat(inMemorySourceAccessor.getAckReceived(), equalTo(null));
    }

    @Test
    void one_pipeline_three_sinks_negative_ack_multiple_records() {
        setUp(ONE_PIPELINE_THREE_SINKS_CONFIGURATION_UNDER_TEST);
        final int numRecords = 100;
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, numRecords);
        inMemorySinkAccessor.setResult(false);

        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            List<Record<Event>> outputRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
            assertThat(outputRecords, not(empty()));
            assertThat(outputRecords.size(), equalTo(3*numRecords));
        });
        assertFalse(inMemorySourceAccessor.getAckReceived());
    }
}
