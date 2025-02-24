/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.integration;

import org.junit.FixMethodOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.test.framework.InMemorySinkAccessor;
import org.opensearch.dataprepper.plugins.test.framework.InMemorySourceAccessor;
import org.opensearch.dataprepper.test.framework.DataPrepperTestRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@FixMethodOrder()
class PipelinesWithAcksIT {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinesWithAcksIT.class);
    private static final String BASE_PATH = "src/integrationTest/resources/org/opensearch/dataprepper/";
    private static final String PIPELINE_BASE_PATH = "src/integrationTest/resources/org/opensearch/dataprepper/pipeline/acknowledgements/";
    private static final String DATA_PREPPER_CONFIG_FILE = BASE_PATH + "configuration/data-prepper-config.yaml";
    private static final String IN_MEMORY_IDENTIFIER = "PipelinesWithAcksIT";
    private static final String SIMPLE_PIPELINE_CONFIGURATION_UNDER_TEST = PIPELINE_BASE_PATH + "simple-test.yaml";
    private static final String TWO_PIPELINES_CONFIGURATION_UNDER_TEST = PIPELINE_BASE_PATH + "two-pipelines-test.yaml";
    private static final String TWO_PARALLEL_PIPELINES_CONFIGURATION_UNDER_TEST = PIPELINE_BASE_PATH + "two-parallel-pipelines-test.yaml";
    private static final String THREE_PIPELINES_CONFIGURATION_UNDER_TEST = PIPELINE_BASE_PATH + "three-pipelines-test.yaml";
    private static final String THREE_PIPELINES_WITH_ROUTE_CONFIGURATION_UNDER_TEST = PIPELINE_BASE_PATH + "three-pipeline-route-test.yaml";
    private static final String THREE_PIPELINES_WITH_UNROUTED_CONFIGURATION_UNDER_TEST = PIPELINE_BASE_PATH + "three-pipeline-unrouted-test.yaml";
    private static final String THREE_PIPELINES_WITH_DEFAULT_ROUTE_CONFIGURATION_UNDER_TEST = PIPELINE_BASE_PATH + "three-pipeline-route-default-test.yaml";
    private static final String THREE_PIPELINES_MULTI_SINK_CONFIGURATION_UNDER_TEST = PIPELINE_BASE_PATH + "three-pipelines-test-multi-sink.yaml";
    private static final String ONE_PIPELINE_THREE_SINKS_CONFIGURATION_UNDER_TEST = PIPELINE_BASE_PATH + "one-pipeline-three-sinks.yaml";
    private static final String ONE_PIPELINE_ACK_EXPIRY_CONFIGURATION_UNDER_TEST = PIPELINE_BASE_PATH + "one-pipeline-ack-expiry-test.yaml";
    private DataPrepperTestRunner dataPrepperTestRunner;
    private InMemorySourceAccessor inMemorySourceAccessor;
    private InMemorySinkAccessor inMemorySinkAccessor;

    void setUp(String configFile) {
        dataPrepperTestRunner = DataPrepperTestRunner.builder()
                .withPipelinesDirectoryOrFile(configFile)
                .withDataPrepperConfigFile(DATA_PREPPER_CONFIG_FILE)
                .build();

	LOG.info("PipelinesWithAcksIT with config file {} started at {}", configFile, Instant.now());
        dataPrepperTestRunner.start();
        inMemorySourceAccessor = dataPrepperTestRunner.getInMemorySourceAccessor();
        inMemorySinkAccessor = dataPrepperTestRunner.getInMemorySinkAccessor();
    }

    @AfterEach
    void tearDown() {
	LOG.info("PipelinesWithAcksIT with stopped at {}", Instant.now());
        dataPrepperTestRunner.stop();
    }

    private List<Record<Event>> createRecords(int numRecords, boolean withStatus) {
        List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            final int max = 600;
            final int min = 100;
            int status = (int)(Math.random() * (max - min + 1) + min);
            Map<String, Object> eventMap = (withStatus) ? 
					Map.of("message", UUID.randomUUID().toString(), "status",  status) :
					Map.of("message", UUID.randomUUID().toString());
            final Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();
            records.add(new Record<>(event));
        }
	return records;
    }

    @Test
    void simple_pipeline_with_single_record() {
        setUp(SIMPLE_PIPELINE_CONFIGURATION_UNDER_TEST);
        final int numRecords = 1;
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, createRecords(numRecords, false));

        await().atMost(40000, TimeUnit.MILLISECONDS)
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
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, createRecords(numRecords, false));

        await().atMost(40000, TimeUnit.MILLISECONDS)
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
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, createRecords(numRecords, false));

        await().atMost(40000, TimeUnit.MILLISECONDS)
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
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, createRecords(numRecords, false));

        await().atMost(40000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            List<Record<Event>> outputRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
            assertThat(outputRecords, not(empty()));
            assertThat(outputRecords.size(), equalTo(numRecords));
        });
        assertTrue(inMemorySourceAccessor.getAckReceived());
    }

    @Test
    void three_pipelines_with_all_unrouted_records() {
        setUp(THREE_PIPELINES_WITH_UNROUTED_CONFIGURATION_UNDER_TEST);
        final int numRecords = 2;
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, createRecords(numRecords, false));

        await().atMost(40000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            assertTrue(inMemorySourceAccessor != null);
            assertTrue(inMemorySourceAccessor.getAckReceived() != null);
        });
        List<Record<Event>> outputRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
        assertThat(outputRecords.size(), equalTo(0));
        assertTrue(inMemorySourceAccessor.getAckReceived());
    }

    @Test
    void three_pipelines_with_route_and_multiple_records() {
        setUp(THREE_PIPELINES_WITH_ROUTE_CONFIGURATION_UNDER_TEST);
        final int numRecords = 10;
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, createRecords(numRecords, true));

        await().atMost(40000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            List<Record<Event>> outputRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
            assertThat(outputRecords, not(empty()));
            assertThat(outputRecords.size(), lessThanOrEqualTo(numRecords));
        });
        assertTrue(inMemorySourceAccessor.getAckReceived());
    }

    @Test
    void three_pipelines_with_default_route_and_multiple_records() {
        setUp(THREE_PIPELINES_WITH_DEFAULT_ROUTE_CONFIGURATION_UNDER_TEST);
        final int numRecords = 10;

        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, createRecords(numRecords, true));

        await().atMost(40000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            List<Record<Event>> outputRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
            assertThat(outputRecords, not(empty()));
            assertThat(outputRecords.size(), equalTo(2*numRecords));
        });
        assertTrue(inMemorySourceAccessor.getAckReceived());
    }

    @Test
    void two_parallel_pipelines_multiple_records() {
        setUp(TWO_PARALLEL_PIPELINES_CONFIGURATION_UNDER_TEST);
        final int numRecords = 100;
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, createRecords(numRecords, false));

        await().atMost(40000, TimeUnit.MILLISECONDS)
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
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, createRecords(numRecords, false));

        await().atMost(40000, TimeUnit.MILLISECONDS)
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
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, createRecords(numRecords, false));

        await().atMost(40000, TimeUnit.MILLISECONDS)
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
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, createRecords(numRecords, false));

        await().atMost(40000, TimeUnit.MILLISECONDS)
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
        inMemorySourceAccessor.submit(IN_MEMORY_IDENTIFIER, createRecords(numRecords, false));
        inMemorySinkAccessor.setResult(false);

        await().atMost(40000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
            List<Record<Event>> outputRecords = inMemorySinkAccessor.get(IN_MEMORY_IDENTIFIER);
            assertThat(outputRecords, not(empty()));
            assertThat(outputRecords.size(), equalTo(3*numRecords));
        });
        assertFalse(inMemorySourceAccessor.getAckReceived());
    }
}
