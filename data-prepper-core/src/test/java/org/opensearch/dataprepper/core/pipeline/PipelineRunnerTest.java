/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import org.junit.jupiter.api.extension.ExtendWith;

import org.opensearch.dataprepper.core.pipeline.common.FutureHelper;
import org.opensearch.dataprepper.core.pipeline.common.FutureHelperResult;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.InternalEventHandle;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@ExtendWith(MockitoExtension.class)
class PipelineRunnerTest {
    private static final int BUFFER_READ_TIMEOUT_MILLIS = 1000;
    private static final String MOCK_PIPELINE_NAME = "Test-Pipeline";

    @Mock
    Pipeline pipeline;
    @Mock
    Buffer buffer;
    @Mock
    Source source;
    @Mock
    Processor processor;
    @Mock
    Sink sink;
    @Mock
    Record record;
    @Mock
    CheckpointState checkpointState;
    @Mock
    Event event;
    @Mock
    EventHandle eventHandle;
    @Mock
    DefaultEventHandle defaultEventHandle;
    private List<Processor> processors;

    private void setupPipeline(boolean shouldEnableAcknowledgements) {
        lenient().when(pipeline.areAcknowledgementsEnabled()).thenReturn(shouldEnableAcknowledgements);
    }

    private PipelineRunnerImpl createObjectUnderTest() {
        return new PipelineRunnerImpl(pipeline, processors);
    }

    @BeforeEach
    void setUp() {
        processors = List.of(processor);
    }

    @Nested
    class ProcessAcknowledgementsTests {
        @BeforeEach
        public void setup() {
            setupPipeline(false);
        }

        @Test
        public void testProcessAcknowledgementsSuccess() {
            List<Event> inputEvents = List.of(event);
            Set<Event> outputEvents = mock(Set.class);
            Collection<Record<Event>> outputRecords = List.of(record);
            // pass an instance of DefaultEventHandle inorder to have (eventHandle instanceof DefaultEventHandle) return true
            when(event.getEventHandle()).thenReturn(defaultEventHandle);
            when(outputRecords.stream().map(Record::getData).collect(Collectors.toSet())).thenReturn(outputEvents);

            PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
            pipelineRunner.processAcknowledgements(inputEvents, outputRecords);
            verify(defaultEventHandle).release(true);
        }

        @Test
        void testProcessAcknowledgementsReleasesMissingEvents() {
            // Create an event with a DefaultEventHandle
            when(event.getEventHandle()).thenReturn(defaultEventHandle);
            List<Event> inputEvents = List.of(event);
            // Create outputRecords that do not contain the first event to simulate a dropped event
            Event differentEvent = mock(Event.class);
            when(record.getData()).thenReturn(differentEvent);
            Collection<Record<Event>> outputRecords = List.of(record);

            PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
            pipelineRunner.processAcknowledgements(inputEvents, outputRecords);
            verify(defaultEventHandle).release(true);
            assertNotSame(event, differentEvent);
        }

        @Test
        void testProcessAcknowledgementsDoesNotReleaseWhenEventsPresent() {
            // Create an event with a DefaultEventHandle.
            when(event.getEventHandle()).thenReturn(defaultEventHandle);
            when(record.getData()).thenReturn(event);
            List<Event> inputEvents = List.of(event);
            // Create outputRecords that contains the first event.
            Collection<Record<Event>> outputRecords = List.of(record);

            PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
            pipelineRunner.processAcknowledgements(inputEvents, outputRecords);
            verify(defaultEventHandle, never()).release(true);
        }

        @Test
        void testProcessAcknowledgementsInvalidEventHandleIncrementsCounter() {
            List<Event> inputEvents = List.of(event);
            Collection<Record<Event>> outputRecords = List.of(record);
            when(event.getEventHandle()).thenReturn(eventHandle);

            try (MockedStatic<PluginMetrics> pluginMetricsStatic = mockStatic(PluginMetrics.class)) {
                final PluginMetrics pluginMetrics = mock(PluginMetrics.class);
                final Counter counter = mock(Counter.class);
                when(pluginMetrics.counter(any())).thenReturn(counter);
                pluginMetricsStatic.when(() -> PluginMetrics.fromNames("PipelineRunner", pipeline.getName()))
                        .thenReturn(pluginMetrics);

                PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
                assertDoesNotThrow(() -> pipelineRunner.processAcknowledgements(inputEvents, outputRecords));

                verify(counter, atLeastOnce()).increment();
            }
        }
    }

    @Nested
    class PublishToSinkTests {
        @BeforeEach
        public void setup() {
            setupPipeline(false);
        }

        @Test
        public void testPublishToSinkHappyCase() {
            final Future<Void> sinkFuture = mock(Future.class);
            List<Future<Void>> sinkFutures = List.of(sinkFuture);
            when(pipeline.publishToSinks(any())).thenReturn(sinkFutures);
            final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
            when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());

            try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
                futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                        .thenReturn(futureHelperResult);
                PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
                boolean postToSinkSuccessful = pipelineRunner.postToSink(pipeline, IntStream.range(0, 5)
                        .mapToObj(i -> new Record("Data " + (i + 1)))
                        .collect(Collectors.toList()));
                assertTrue(postToSinkSuccessful);
            }

            verify(pipeline).publishToSinks(anyCollection());
        }

        @Test
        public void testPublishToSinkFailure() {
            final Future<Void> sinkFuture = mock(Future.class);
            List<Future<Void>> sinkFutures = List.of(sinkFuture);
            List<ExecutionException> executionExceptions = List.of(mock(ExecutionException.class));
            when(pipeline.publishToSinks(any())).thenReturn(sinkFutures);
            final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
            when(futureHelperResult.getFailedReasons()).thenReturn(executionExceptions);

            try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
                futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                        .thenReturn(futureHelperResult);
                PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
                boolean postToSinkSuccessful = pipelineRunner.postToSink(pipeline, IntStream.range(0, 5)
                        .mapToObj(i -> new Record("Data " + (i + 1)))
                        .collect(Collectors.toList()));
                assertFalse(postToSinkSuccessful);
            }

            verify(pipeline).publishToSinks(anyCollection());
        }
    }

    @Nested
    class ReadFromBufferTests {
        @BeforeEach
        public void setup() {
            setupPipeline(false);
        }

        @Test
        public void testReadFromBufferReturnsNoRecords() {
            final List<Record<String>> records = new ArrayList<>();
            final Map.Entry<Collection, CheckpointState> readResult = Map.entry(records, checkpointState);
            when(buffer.read(pipeline.getReadBatchTimeoutInMillis())).thenReturn(readResult);

            PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
            final Map.Entry<Collection, CheckpointState> firstReadResult = pipelineRunner.readFromBuffer(buffer, pipeline);
            final Map.Entry<Collection, CheckpointState> secondReadResult = pipelineRunner.readFromBuffer(buffer, pipeline);
            assertTrue(firstReadResult.getKey().isEmpty());
            assertTrue(secondReadResult.getKey().isEmpty());

            verify(buffer, atLeastOnce()).read(anyInt());
            verify(pipeline, atLeastOnce()).getReadBatchTimeoutInMillis();
        }

        @Test
        public void testReadFromBufferReturnsCorrectRecords() {
            final List<Record<String>> records = List.of(record);
            final Map.Entry<Collection, CheckpointState> readResult = Map.entry(records, checkpointState);
            when(pipeline.getReadBatchTimeoutInMillis()).thenReturn(BUFFER_READ_TIMEOUT_MILLIS);
            when(buffer.read(pipeline.getReadBatchTimeoutInMillis())).thenReturn(readResult);
            PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
            final Map.Entry<Collection, CheckpointState> returnedReadResult = pipelineRunner.readFromBuffer(buffer, pipeline);
            assertFalse(returnedReadResult.getKey().isEmpty());

            verify(buffer, atLeastOnce()).read(anyInt());
            verify(pipeline, atLeastOnce()).getReadBatchTimeoutInMillis();
        }
    }

    @Nested
    class RunProcessorsAndProcessAcknowledgementsTests {
        @Test
        void testProcessWorkerWithProcessorsNotHoldingEvents() {
            setupPipeline(true);
            when(defaultEventHandle.release(true)).thenReturn(true);
            lenient().when(event.getEventHandle()).thenReturn(defaultEventHandle);
            when(record.getData()).thenReturn(event);
            final List<Record> records = new ArrayList<>();
            records.add(record);

            when(processor.holdsEvents()).thenReturn(false);
            when(processor.execute(records)).thenReturn(List.of());


            final PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
            pipelineRunner.runProcessorsAndProcessAcknowledgements(processors, records);

            verify(defaultEventHandle, atLeast(1)).release(true);
        }

        @Test
        void testProcessWorkerWithProcessorsHoldingEventsWithAcknowledgementsEnabled() {
            setupPipeline(true);
            final List<Record> records = List.of(record);
            when(processor.holdsEvents()).thenReturn(true);
            when(processor.execute(records)).thenReturn(List.of());
            lenient().when(event.getEventHandle()).thenReturn(eventHandle);
            when(record.getData()).thenReturn(event);

            final PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
            pipelineRunner.runProcessorsAndProcessAcknowledgements(List.of(processor), records);

            verify(eventHandle, never()).release(true);
        }

        @Test
        void testRunProcessorsAndProcessAcknowledgementsWithProcessorDroppingAllRecordsAndAcknowledgmentsEnabledIsHandledProperly() {
            setupPipeline(true);
            final List<Record> records = List.of(record);
            when(processor.holdsEvents()).thenReturn(true);
            when(processor.execute(records)).thenReturn(Collections.emptyList());
            final Processor secondProcessor = mock(Processor.class);
            when(secondProcessor.execute(Collections.emptyList())).thenReturn(Collections.emptyList());
            List<Processor> processors = List.of(processor, secondProcessor);

            lenient().when(event.getEventHandle()).thenReturn(eventHandle);
            when(record.getData()).thenReturn(event);

            final PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
            Collection result = pipelineRunner.runProcessorsAndProcessAcknowledgements(processors, records);

            verify(eventHandle, never()).release(true);
            assertTrue(result.isEmpty());
        }

        @Test
        public void testrunProcessorsAndProcessAcknowledgementsWhenNoProcessors() {
            setupPipeline(false);
            PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
            Collection records = pipelineRunner.runProcessorsAndProcessAcknowledgements(new ArrayList<>(), new ArrayList<Record<Event>>());

            assertTrue(records.isEmpty());
            verify(processor, never()).execute(any());
        }

        @Test
        void testRunProcessorsAndProcessAcknowledgementsThrowsException() {
            List<Record<Event>> inputRecords = new ArrayList<>();
            final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
            ((InternalEventHandle)defaultEventHandle).addAcknowledgementSet(acknowledgementSet);
            inputRecords.add(record);
            setupPipeline(false);
            // Have the processor throw an exception
            when(processor.execute(inputRecords)).thenThrow(new RuntimeException());;
            final Processor skippedProcessor = mock(Processor.class);
            List<Processor> processors = List.of(processor, skippedProcessor);
            PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
            Collection<?> result = pipelineRunner.runProcessorsAndProcessAcknowledgements(
                    processors, inputRecords);

            verify(defaultEventHandle, never()).release(anyBoolean());
            verify(skippedProcessor, never()).execute(inputRecords);
            assertTrue(result.isEmpty());
        }

        @Test
        void testRunProcessorsAndProcessAcknowledgementsThrowingExceptionWithAcknowledgmentsEnabledIsHandledProperly() {
            // Create an event with a DefaultEventHandle.
            when(event.getEventHandle()).thenReturn(defaultEventHandle);
            when(record.getData()).thenReturn(event);
            List<Record<Event>> inputRecords = new ArrayList<>();
            inputRecords.add(record);
            setupPipeline(true);
            // Have the processor throw an exception
            when(processor.execute(inputRecords)).thenThrow(new RuntimeException());
            PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
            Collection<?> result = pipelineRunner.runProcessorsAndProcessAcknowledgements(
                    Collections.singletonList(processor), inputRecords);

            verify(defaultEventHandle).release(true);
            assertTrue(result.isEmpty());
        }
    }

    @Nested
    class RunAllProcessorsAndPublishToSinksTests {
        @Test
        void testRunAllProcessorsAndPublishToSinkWithAcknowledgmentsEnabled() {
            when(record.getData()).thenReturn(event);
            Collection recordsList = new ArrayList<>();
            recordsList.add(record);
            setupPipeline(true);
            // Set up additional pipeline behavior
            when(pipeline.getBuffer()).thenReturn(buffer);
            when(pipeline.getReadBatchTimeoutInMillis()).thenReturn(BUFFER_READ_TIMEOUT_MILLIS);
            when(pipeline.getName()).thenReturn(MOCK_PIPELINE_NAME);
            when(pipeline.publishToSinks(anyCollection())).thenReturn(
                    Collections.singletonList(CompletableFuture.completedFuture(null)));

            Map.Entry<Collection, CheckpointState> entry =
                    new AbstractMap.SimpleEntry<>(recordsList, checkpointState);
            when(buffer.read(BUFFER_READ_TIMEOUT_MILLIS)).thenReturn(entry);
            when(processor.execute(recordsList)).thenReturn(recordsList);
            PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
            pipelineRunner.runAllProcessorsAndPublishToSinks();

            verify(processor).execute(recordsList);
            verify(pipeline).publishToSinks(recordsList);
            verify(buffer).checkpoint(checkpointState);
        }

        @Test
        void testRunAllProcessorsAndPublishToSinksHappyPathWithoutAcknowledgments() {
            Collection recordsList = new ArrayList<>();
            recordsList.add(record);
            setupPipeline(false);
            // Set up additional pipeline behavior
            when(pipeline.getBuffer()).thenReturn(buffer);
            when(pipeline.getReadBatchTimeoutInMillis()).thenReturn(BUFFER_READ_TIMEOUT_MILLIS);
            when(pipeline.getName()).thenReturn(MOCK_PIPELINE_NAME);
            when(pipeline.publishToSinks(anyCollection())).thenReturn(
                    Collections.singletonList(CompletableFuture.completedFuture(null)));

            Map.Entry<Collection, CheckpointState> entry =
                    new AbstractMap.SimpleEntry<>(recordsList, checkpointState);
            when(buffer.read(BUFFER_READ_TIMEOUT_MILLIS)).thenReturn(entry);
            when(processor.execute(recordsList)).thenReturn(recordsList);
            PipelineRunnerImpl pipelineRunner = createObjectUnderTest();
            pipelineRunner.runAllProcessorsAndPublishToSinks();

            verify(processor).execute(recordsList);
            verify(pipeline).publishToSinks(recordsList);
            verify(buffer).checkpoint(checkpointState);
        }
    }
}