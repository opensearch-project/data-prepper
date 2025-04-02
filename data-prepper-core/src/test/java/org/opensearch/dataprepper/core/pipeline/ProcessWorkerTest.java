package org.opensearch.dataprepper.core.pipeline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.processor.Processor;

import java.time.Duration;
import java.util.List;

@ExtendWith(MockitoExtension.class)
public class ProcessWorkerTest {
    @Mock
    private Pipeline pipeline;

    @Mock
    private Processor processor;

    @Mock
    private Buffer buffer;

    private PipelineRunnerImpl pipelineRunner;

    private List<Processor> processors;

    @BeforeEach
    void setup() {
        when(pipeline.isStopRequested()).thenReturn(false).thenReturn(true);
        when(pipeline.getPeerForwarderDrainTimeout()).thenReturn(Duration.ofMillis(100));
        when(buffer.isEmpty()).thenReturn(true);
    }

    private ProcessWorker createObjectUnderTest() {
        try(final MockedConstruction<PipelineRunnerImpl> ignored = mockConstruction(PipelineRunnerImpl.class, (mock, context) -> {
            pipelineRunner = mock;
        })) {
            return new ProcessWorker(buffer, processors, pipeline);
        }
    }

    @Test
    void testProcessWorkerHappyPath() {
        when(processor.isReadyForShutdown()).thenReturn(true);
        processors = List.of(processor);

        final ProcessWorker processWorker = createObjectUnderTest();
        doNothing().when(pipelineRunner).runAllProcessorsAndPublishToSinks();

        processWorker.run();

        verify(pipelineRunner, atLeastOnce()).runAllProcessorsAndPublishToSinks();
    }

    @Test
    void testProcessWorkerInitializesPipelineRunnerCorrectly() {
        when(processor.isReadyForShutdown()).thenReturn(true);
        processors = List.of(processor);

        final ProcessWorker processWorker = createObjectUnderTest();
        processWorker.run();

        verify(pipelineRunner, atLeastOnce()).runAllProcessorsAndPublishToSinks();
    }

    @Test
    void testProcessWorkerShutdownProcessPreparesProcessorsForShutdown() {
        processors = List.of(processor);
        when(pipeline.isStopRequested()).thenReturn(false, true);
        when(pipeline.getPeerForwarderDrainTimeout()).thenReturn(Duration.ofMillis(1));
        when(buffer.isEmpty()).thenReturn(true);
        when(processor.isReadyForShutdown()).thenReturn(true);

        final ProcessWorker processWorker = createObjectUnderTest();

        doNothing().when(pipelineRunner).runAllProcessorsAndPublishToSinks();

        processWorker.run();

        verify(processor, atLeastOnce()).prepareForShutdown();
    }

    @Test
    void testProcessWorkerShutdownProcessWaitsUntilBufferEmpty() {
        processors = List.of(processor);
        // First call returns false, then true
        when(pipeline.isStopRequested()).thenReturn(false, true);
        when(pipeline.getPeerForwarderDrainTimeout()).thenReturn(Duration.ofMillis(1));
        when(buffer.isEmpty()).thenReturn(false, true);
        when(processor.isReadyForShutdown()).thenReturn(true);

        final ProcessWorker processWorker = createObjectUnderTest();

        doNothing().when(pipelineRunner).runAllProcessorsAndPublishToSinks();
        processWorker.run();

        // Verify multiple invocations due to the wait loop in shutdown phase 2.
        verify(pipelineRunner, atLeast(2)).runAllProcessorsAndPublishToSinks();
    }

    @Test
    void testProcessWorkerShutdownProcessForceStopReadingBuffers() {
        processors = List.of(processor);
        when(pipeline.isStopRequested()).thenReturn(false, true);
        when(pipeline.getPeerForwarderDrainTimeout()).thenReturn(Duration.ofMillis(1));
        when(buffer.isEmpty()).thenReturn(false);
        when(pipeline.isForceStopReadingBuffers()).thenReturn(true);
        when(processor.isReadyForShutdown()).thenReturn(true);

        final ProcessWorker processWorker = createObjectUnderTest();

        doNothing().when(pipelineRunner).runAllProcessorsAndPublishToSinks();
        processWorker.run();

        verify(pipelineRunner, atLeastOnce()).runAllProcessorsAndPublishToSinks();
    }
}
