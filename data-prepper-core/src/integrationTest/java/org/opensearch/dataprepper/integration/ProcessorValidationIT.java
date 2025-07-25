/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.BaseEventsTrackingProcessor;
import org.opensearch.dataprepper.plugins.InMemorySinkAccessor;
import org.opensearch.dataprepper.plugins.InMemorySourceAccessor;
import org.opensearch.dataprepper.plugins.BasicEventsTrackingTestProcessor;
import org.opensearch.dataprepper.plugins.SingleThreadEventsTrackingTestProcessor;
import org.opensearch.dataprepper.test.framework.DataPrepperTestRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Integration tests for validating processor behavior in pipelines and to verify that
 * events are processed exactly once even while using multiple workers.
 */
class ProcessorValidationIT {
    private static final String IN_MEMORY_IDENTIFIER = "ProcessorValidationIT";
    private static final int BATCH_SIZE = 5;
    private static final int TOTAL_EVENTS = 100;
    private static final int WAIT_TIMEOUT_SECONDS = 10;

    private DataPrepperTestRunner testRunner;
    private InMemorySourceAccessor sourceAccessor;
    private InMemorySinkAccessor sinkAccessor;
    private String pipelineType;

    @BeforeEach
    void setUp() {
        new BasicEventsTrackingTestProcessor().reset();
        new SingleThreadEventsTrackingTestProcessor().reset();
        SingleThreadEventsTrackingTestProcessor.getProcessors().clear();
    }

    @AfterEach
    void tearDown() {
        if (testRunner != null) {
            testRunner.stop();
        }
    }

    /**
     * Parameterized test that validates event processing across different pipeline configurations.
     *
     * @param pipelineType The type of pipeline configuration to test
     * @param testName A descriptive name for the test scenario
     * @param numberOfBatches Number of batches to send to the pipeline
     * @param eventsPerBatch Number of events in each batch
     * @param expectedTotalEvents Total number of events expected to be processed
     */
    @ParameterizedTest(name = "{index} - {0} - {1}")
    @ArgumentsSource(WithoutSingleThreadParametersArgumentsProvider.class)
    void test_events_processed_when_no_SingleThread_processors(String pipelineType, String testName, List<BaseEventsTrackingProcessor> processorEventStores, int numberOfBatches, int eventsPerBatch, int expectedTotalEvents) {
        this.pipelineType = pipelineType;
        initializeTestRunner();
        List<List<Record<Event>>> batches = createBatches(numberOfBatches, eventsPerBatch);
        batches.forEach(batch -> sourceAccessor.submit(IN_MEMORY_IDENTIFIER, batch));

        verifyProcessingResults(pipelineType, expectedTotalEvents, eventsPerBatch, processorEventStores);
    }

    @ParameterizedTest(name = "{index} - {0} - {1}")
    @ArgumentsSource(WithSingleThreadParametersArgumentsProvider.class)
    void test_events_processed_when_SingleThread_processor(String pipelineType, String testName, List<BaseEventsTrackingProcessor> processorEventStores, int numberOfBatches, int eventsPerBatch, int expectedTotalEvents) {
        this.pipelineType = pipelineType;
        initializeTestRunner();
        List<List<Record<Event>>> batches = createBatches(numberOfBatches, eventsPerBatch);
        batches.forEach(batch -> sourceAccessor.submit(IN_MEMORY_IDENTIFIER, batch));

        verifyProcessingResults(pipelineType, expectedTotalEvents, eventsPerBatch, processorEventStores);

        verifySingleThreadUsage();
    }

    static class WithSingleThreadParametersArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    arguments("single-thread-processor-pipeline",
                            "SingleBatch",
                            List.of(new SingleThreadEventsTrackingTestProcessor()),
                            1,
                            TOTAL_EVENTS,
                            TOTAL_EVENTS
                    ),
                    arguments("single-thread-processor-pipeline",
                            "MultipleBatches",
                            List.of(new SingleThreadEventsTrackingTestProcessor()),
                            BATCH_SIZE,
                            TOTAL_EVENTS,
                            BATCH_SIZE * TOTAL_EVENTS
                    ),
                    arguments("multi-processor-pipeline",
                            "SingleBatch",
                            List.of(new SingleThreadEventsTrackingTestProcessor(), new BasicEventsTrackingTestProcessor()),
                            1,
                            TOTAL_EVENTS,
                            TOTAL_EVENTS
                    ),
                    arguments("multi-processor-pipeline",
                            "MultipleBatches",
                            List.of(new SingleThreadEventsTrackingTestProcessor(), new BasicEventsTrackingTestProcessor()),
                            BATCH_SIZE,
                            TOTAL_EVENTS,
                            BATCH_SIZE * TOTAL_EVENTS
                    )
            );
        }
    }

    static class WithoutSingleThreadParametersArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    arguments("basic-processor-pipeline",
                            "SingleBatch",
                            List.of(new BasicEventsTrackingTestProcessor()),
                            1,
                            TOTAL_EVENTS,
                            TOTAL_EVENTS
                    ),
                    arguments("basic-processor-pipeline",
                            "MultipleBatches",
                            List.of(new BasicEventsTrackingTestProcessor()),
                            BATCH_SIZE,
                            TOTAL_EVENTS,
                            BATCH_SIZE * TOTAL_EVENTS
                    )
            );
        }
    }

    /**
     * Initializes the DataPrepper test runner with the appropriate pipeline configuration.
     * Sets up source and sink accessors for test data input and output.
     */
    private void initializeTestRunner() {
        String pipelineFile = pipelineType + ".yaml";
        testRunner = DataPrepperTestRunner.builder()
                .withPipelinesDirectoryOrFile(pipelineFile)
                .build();
        sourceAccessor = testRunner.getInMemorySourceAccessor();
        sinkAccessor = testRunner.getInMemorySinkAccessor();
        testRunner.start();
    }

    /**
     * Verifies that events were processed correctly by the pipeline.
     *
     * @param pipelineType         The type of pipeline being tested
     * @param expectedTotalEvents  Total number of events expected to be processed
     * @param eventsPerBatch       Number of events in each batch
     * @param processorEventStores
     */
    private void verifyProcessingResults(String pipelineType, int expectedTotalEvents, int eventsPerBatch, List<BaseEventsTrackingProcessor> processorEventStores) {
        // Wait for all events to be processed
        await().atMost(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(
                        sinkAccessor.get(IN_MEMORY_IDENTIFIER).size(),
                        equalTo(expectedTotalEvents)));

        List<Record<Event>> outputRecords = sinkAccessor.get(IN_MEMORY_IDENTIFIER);
        assertThat(outputRecords.size(), equalTo(expectedTotalEvents));

        // Verify each processor in the pipeline processed events
        for (BaseEventsTrackingProcessor processor : processorEventStores) {
            String processorName = processor.getName();
            Map<String, AtomicInteger> processedEventsMap = processor.getEventsMap();

            verifyEventProcessing(processedEventsMap, outputRecords, expectedTotalEvents, processorName);
        }

        verifyWorkerThreads(outputRecords, processorEventStores);

        int numberOfBatches = expectedTotalEvents / eventsPerBatch;
        for (int batch = 0; batch < numberOfBatches; batch++) {
            int finalBatch = batch;
            List<Record<Event>> batchRecords = outputRecords.stream()
                    .filter(record -> record.getData().get("batch", Integer.class) == finalBatch)
                    .collect(Collectors.toList());
            assertThat(batchRecords.size(), equalTo(eventsPerBatch));
        }
    }

    private static void verifySingleThreadUsage() {
        List<SingleThreadEventsTrackingTestProcessor> singleThreadProcessors = SingleThreadEventsTrackingTestProcessor.getProcessors();
        assertThat(singleThreadProcessors.size(), equalTo(4));
        assertAll(
                () -> assertThat(singleThreadProcessors.get(0).getThreadsUsing().size(), equalTo(1)),
                () -> assertThat(singleThreadProcessors.get(1).getThreadsUsing().size(), equalTo(1)),
                () -> assertThat(singleThreadProcessors.get(2).getThreadsUsing().size(), equalTo(1)),
                () -> assertThat(singleThreadProcessors.get(3).getThreadsUsing().size(), equalTo(1))
        );

        assertAll(
                () -> assertThat(singleThreadProcessors.get(0).getNumberOfProcessWorkersFromPipelineDescription(), equalTo(4)),
                () -> assertThat(singleThreadProcessors.get(1).getNumberOfProcessWorkersFromPipelineDescription(), equalTo(4)),
                () -> assertThat(singleThreadProcessors.get(2).getNumberOfProcessWorkersFromPipelineDescription(), equalTo(4)),
                () -> assertThat(singleThreadProcessors.get(3).getNumberOfProcessWorkersFromPipelineDescription(), equalTo(4))
        );
    }

    /**
     * Verifies that each event was processed exactly once by the specified processor.
     * Checks both the processor's internal tracking map and the event metadata.
     *
     * @param processedEventsMap Map tracking which events were processed by the processor
     * @param outputRecords List of output records from the pipeline
     * @param expectedTotalEvents Total number of events expected to be processed
     * @param processorType Name of the processor being verified
     */
    private void verifyEventProcessing(Map<String, AtomicInteger> processedEventsMap,
                                       List<Record<Event>> outputRecords,
                                       int expectedTotalEvents,
                                       String processorType) {
        assertThat("Output records list should not be empty", outputRecords.size(), greaterThanOrEqualTo(1));
        String countField = processorType + "_processed_count";
        for (Record<Event> record : outputRecords) {
            String id = record.getData().get("id", String.class);
            AtomicInteger count = processedEventsMap.get(id);

            assertThat("Event with ID " + id + " should be processed exactly once by " + processorType,
                    count.get(), equalTo(1));
            assertThat("Event processing count should be 1 for " + processorType,
                    record.getData().get(countField, Integer.class), equalTo(1));
        }

        assertThat("All events should be processed by " + processorType,
                processedEventsMap.size(), equalTo(expectedTotalEvents));
    }

    /**
     * Verifies that worker threads were assigned correctly based on processor configuration.
     * Ensures that at least one worker thread was used for processing.
     *
     * @param outputRecords List of output records from the pipeline
     * @param processors List of processors in the pipeline
     */
    private void verifyWorkerThreads(List<Record<Event>> outputRecords, List<BaseEventsTrackingProcessor> processors) {
        Set<String> threadNames = outputRecords.stream()
                .map(Record::getData)
                .flatMap(event -> processors.stream()
                        .map(processor -> {
                            String processorName = processor.getName();
                            return event.get(processorName + "_processed_by_thread", String.class);
                        })
                        .filter(threadName -> threadName != null))
                .collect(Collectors.toSet());

        assertThat("There should be at least one worker thread",
                threadNames.size(), greaterThanOrEqualTo(1));
    }

    /**
     * Creates a list of event records with unique IDs and sequential numbering.
     *
     * @param count Number of records to create
     * @return List of event records
     */
    private List<Record<Event>> createRecords(int count) {
        List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new Record<>(createEvent(i)));
        }
        return records;
    }

    /**
     * Creates multiple batches of event records.
     * Each batch is assigned a batch number, and events within each batch
     * are given sequential numbers across all batches.
     *
     * @param batchSize Number of batches to create
     * @param eventsPerBatch Number of events in each batch
     * @return List of batches, where each batch is a list of event records
     */
    private List<List<Record<Event>>> createBatches(int batchSize, int eventsPerBatch) {
        List<List<Record<Event>>> batches = new ArrayList<>();
        for (int batch = 0; batch < batchSize; batch++) {
            int batchOffset = batch * eventsPerBatch;
            int currentBatch = batch;
            List<Record<Event>> batchRecords = createRecords(eventsPerBatch).stream()
                    .map(record -> {
                        Event event = record.getData();
                        event.put("sequence", batchOffset + event.get("sequence", Integer.class));
                        event.put("batch", currentBatch);
                        return new Record<>(event);
                    })
                    .collect(Collectors.toList());
            batches.add(batchRecords);
        }
        return batches;
    }

    /**
     * Creates a single event with a unique ID and sequence number.
     *
     * @param sequence Sequence number to assign to the event
     * @return The created event
     */
    private Event createEvent(int sequence) {
        String eventId = UUID.randomUUID().toString();
        Event event = JacksonEvent.fromMessage(eventId);
        event.put("id", eventId);
        event.put("sequence", sequence);
        return event;
    }
}
