package org.opensearch.dataprepper.pipeline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.InternalEventHandle;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.pipeline.common.FutureHelper;
import org.opensearch.dataprepper.pipeline.common.FutureHelperResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ProcessWorkerTest {

    @Mock
    private Pipeline pipeline;

    @Mock
    private Buffer buffer;

    @Mock
    private Source source;

    private List<Future<Void>> sinkFutures;

    private List<Processor> processors;

    @BeforeEach
    void setup() {
        when(pipeline.isStopRequested()).thenReturn(false).thenReturn(true);
        when(source.areAcknowledgementsEnabled()).thenReturn(false);
        when(pipeline.getSource()).thenReturn(source);
        when(buffer.isEmpty()).thenReturn(true);
        when(pipeline.getPeerForwarderDrainTimeout()).thenReturn(Duration.ofMillis(100));
        when(pipeline.getReadBatchTimeoutInMillis()).thenReturn(500);

        final Future<Void> sinkFuture = mock(Future.class);
        sinkFutures = List.of(sinkFuture);
        when(pipeline.publishToSinks(any())).thenReturn(sinkFutures);
    }

    private ProcessWorker createObjectUnderTest() {
        return new ProcessWorker(buffer, processors, pipeline);
    }

    @Test
    void testProcessWorkerHappyPath() {

        final List<Record> records = List.of(mock(Record.class));
        final CheckpointState checkpointState = mock(CheckpointState.class);
        final Map.Entry<Collection, CheckpointState> readResult = Map.entry(records, checkpointState);
        when(buffer.read(pipeline.getReadBatchTimeoutInMillis())).thenReturn(readResult);

        final Processor processor = mock(Processor.class);
        when(processor.execute(records)).thenReturn(records);
        when(processor.isReadyForShutdown()).thenReturn(true);
        processors = List.of(processor);

        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());


        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = createObjectUnderTest();

            processWorker.run();
        }
    }

    @Test
    void testProcessWorkerHappyPathWithAcknowledgments() {

        when(source.areAcknowledgementsEnabled()).thenReturn(true);

        final List<Record<Event>> records = new ArrayList<>();
        final Record<Event> mockRecord = mock(Record.class);
        final Event mockEvent = mock(Event.class);
        final EventHandle eventHandle = mock(DefaultEventHandle.class);
        when(mockRecord.getData()).thenReturn(mockEvent);
        when(mockEvent.getEventHandle()).thenReturn(eventHandle);

        records.add(mockRecord);

        final CheckpointState checkpointState = mock(CheckpointState.class);
        final Map.Entry<Collection, CheckpointState> readResult = Map.entry(records, checkpointState);
        when(buffer.read(pipeline.getReadBatchTimeoutInMillis())).thenReturn(readResult);

        final Processor processor = mock(Processor.class);
        when(processor.execute(records)).thenReturn(records);
        when(processor.isReadyForShutdown()).thenReturn(true);
        processors = List.of(processor);

        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());


        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = createObjectUnderTest();

            processWorker.run();
        }
    }

    @Test
    void testProcessWorkerWithProcessorThrowingExceptionIsCaughtProperly() {

        final List<Record> records = List.of(mock(Record.class));
        final CheckpointState checkpointState = mock(CheckpointState.class);
        final Map.Entry<Collection, CheckpointState> readResult = Map.entry(records, checkpointState);
        when(buffer.read(pipeline.getReadBatchTimeoutInMillis())).thenReturn(readResult);

        final Processor processor = mock(Processor.class);
        when(processor.execute(records)).thenThrow(RuntimeException.class);
        when(processor.isReadyForShutdown()).thenReturn(true);

        final Processor skippedProcessor = mock(Processor.class);
        when(skippedProcessor.isReadyForShutdown()).thenReturn(true);
        processors = List.of(processor, skippedProcessor);

        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());


        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = createObjectUnderTest();

            processWorker.run();
        }

        verify(skippedProcessor, never()).execute(any());
    }

    @Test
    void testProcessWorkerWithProcessorThrowingExceptionAndAcknowledgmentsEnabledIsHandledProperly() {

        when(source.areAcknowledgementsEnabled()).thenReturn(true);

        final List<Record<Event>> records = new ArrayList<>();
        final Record<Event> mockRecord = mock(Record.class);
        final Event mockEvent = mock(Event.class);
        final EventHandle eventHandle = mock(DefaultEventHandle.class);
        final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        ((InternalEventHandle)eventHandle).addAcknowledgementSet(acknowledgementSet);
        when(mockRecord.getData()).thenReturn(mockEvent);
        when(mockEvent.getEventHandle()).thenReturn(eventHandle);

        records.add(mockRecord);

        final CheckpointState checkpointState = mock(CheckpointState.class);
        final Map.Entry<Collection, CheckpointState> readResult = Map.entry(records, checkpointState);
        when(buffer.read(pipeline.getReadBatchTimeoutInMillis())).thenReturn(readResult);

        final Processor processor = mock(Processor.class);
        when(processor.execute(records)).thenThrow(RuntimeException.class);
        when(processor.isReadyForShutdown()).thenReturn(true);

        final Processor skippedProcessor = mock(Processor.class);
        when(skippedProcessor.isReadyForShutdown()).thenReturn(true);
        processors = List.of(processor, skippedProcessor);

        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());


        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = createObjectUnderTest();

            processWorker.run();
        }

        verify(skippedProcessor, never()).execute(any());
    }

    @Test
    void testProcessWorkerWithProcessorDroppingAllRecordsAndAcknowledgmentsEnabledIsHandledProperly() {

        when(source.areAcknowledgementsEnabled()).thenReturn(true);

        final List<Record<Event>> records = new ArrayList<>();
        final Record<Event> mockRecord = mock(Record.class);
        final Event mockEvent = mock(Event.class);
        final EventHandle eventHandle = mock(DefaultEventHandle.class);
        final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        ((InternalEventHandle)eventHandle).addAcknowledgementSet(acknowledgementSet);
        when(mockRecord.getData()).thenReturn(mockEvent);
        when(mockEvent.getEventHandle()).thenReturn(eventHandle);

        records.add(mockRecord);

        final CheckpointState checkpointState = mock(CheckpointState.class);
        final Map.Entry<Collection, CheckpointState> readResult = Map.entry(records, checkpointState);
        when(buffer.read(pipeline.getReadBatchTimeoutInMillis())).thenReturn(readResult);

        final Processor processor = mock(Processor.class);
        when(processor.execute(records)).thenReturn(Collections.emptyList());
        when(processor.isReadyForShutdown()).thenReturn(true);

        final Processor secondProcessor = mock(Processor.class);
        when(secondProcessor.isReadyForShutdown()).thenReturn(true);
        when(secondProcessor.execute(Collections.emptyList())).thenReturn(Collections.emptyList());
        processors = List.of(processor, secondProcessor);

        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());


        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = createObjectUnderTest();

            processWorker.run();
        }
    }
}
