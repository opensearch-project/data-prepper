package org.opensearch.dataprepper.core.pipeline;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.opensearch.dataprepper.core.pipeline.exceptions.InvalidEventHandleException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
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

    @Mock
    private PipelineRunnerImpl pipelineRunner;

    private List<Processor> processors;

    @BeforeEach
    void setup() {
        when(pipeline.isStopRequested()).thenReturn(false).thenReturn(true);
        when(pipeline.getPeerForwarderDrainTimeout()).thenReturn(Duration.ofMillis(100));
        when(buffer.isEmpty()).thenReturn(true);
    }

    private ProcessWorker createObjectUnderTest() {
        return createObjectUnderTest(true);
    }

    private ProcessWorker createObjectUnderTest(boolean withPipelineRunner) {
        if (withPipelineRunner) {
            return new ProcessWorker(buffer, processors, pipeline, pipelineRunner);
        }
        return new ProcessWorker(buffer, processors, pipeline);
    }

    @Test
    void testProcessWorkerHappyPath() {
        when(processor.isReadyForShutdown()).thenReturn(true);
        doNothing().when(pipelineRunner).runAllProcessorsAndPublishToSinks();
        processors = List.of(processor);

       final ProcessWorker processWorker = createObjectUnderTest();
       processWorker.run();

        verify(pipelineRunner, atLeastOnce()).runAllProcessorsAndPublishToSinks();
    }

    @Test
    void testProcessWorkerInitializesPipelineRunnerCorrectly() {
        when(processor.isReadyForShutdown()).thenReturn(true);
        processors = List.of(processor);

        try (MockedConstruction<PipelineRunnerImpl> pipelineRunnerMockedConstruction = mockConstruction(PipelineRunnerImpl.class,
                (mock, context) -> doNothing().when(mock).runAllProcessorsAndPublishToSinks())) {
            final ProcessWorker processWorker = createObjectUnderTest(false);
            processWorker.run();

            pipelineRunnerMockedConstruction.constructed().forEach(pipelineRunner -> {
                verify(pipelineRunner, atLeastOnce()).runAllProcessorsAndPublishToSinks();
            });
        }
    }

    @Test
    void testProcessWorkerShutdownProcessPreparesProcessorsForShutdown() {
        processors = List.of(processor);
        when(pipeline.isStopRequested()).thenReturn(false, true);
        when(pipeline.getPeerForwarderDrainTimeout()).thenReturn(Duration.ofMillis(1));
        when(buffer.isEmpty()).thenReturn(true);
        when(processor.isReadyForShutdown()).thenReturn(true);
        doNothing().when(pipelineRunner).runAllProcessorsAndPublishToSinks();

        final ProcessWorker processWorker = createObjectUnderTest();
        processWorker.run();

        verify(processor, atLeastOnce()).prepareForShutdown();
    }

    @Test
    void testProcessWorkerInvalidEventHandleIncrementsCounter() {
        processors = List.of(processor);
        when(pipeline.isStopRequested()).thenReturn(false, true);
        when(pipeline.getPeerForwarderDrainTimeout()).thenReturn(Duration.ofMillis(1));
        when(buffer.isEmpty()).thenReturn(true);
        when(processor.isReadyForShutdown()).thenReturn(true);

        // Create a pipelineRunner mock that throws an InvalidEventHandleException on first call.
        doThrow(new InvalidEventHandleException(""))
                .doNothing()
                .when(pipelineRunner).runAllProcessorsAndPublishToSinks();

        try (MockedStatic<PluginMetrics> pluginMetricsStatic = mockStatic(PluginMetrics.class)) {
            final PluginMetrics pluginMetrics = mock(PluginMetrics.class);
            final Counter counter = mock(Counter.class);
            when(pluginMetrics.counter(any())).thenReturn(counter);
            pluginMetricsStatic.when(() -> PluginMetrics.fromNames("ProcessWorker", pipeline.getName()))
                    .thenReturn(pluginMetrics);

            final ProcessWorker processWorker = createObjectUnderTest();
            processWorker.run();

            verify(counter, atLeastOnce()).increment();
        }
    }

    @Test
    void testProcessWorkerShutdownProcessWaitsUntilBufferEmpty() {
        processors = List.of(processor);
        // First call returns false, then true
        when(pipeline.isStopRequested()).thenReturn(false, true);
        when(pipeline.getPeerForwarderDrainTimeout()).thenReturn(Duration.ofMillis(1));
        when(buffer.isEmpty()).thenReturn(false, true);
        when(processor.isReadyForShutdown()).thenReturn(true);
        doNothing().when(pipelineRunner).runAllProcessorsAndPublishToSinks();

        final ProcessWorker processWorker = createObjectUnderTest();
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
        doNothing().when(pipelineRunner).runAllProcessorsAndPublishToSinks();

        final ProcessWorker processWorker = createObjectUnderTest();
        processWorker.run();

        verify(pipelineRunner, atLeastOnce()).runAllProcessorsAndPublishToSinks();
    }
}
