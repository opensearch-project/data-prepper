/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.source.coordinator.UsesSourceCoordination;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.parser.DataFlowComponent;
import org.opensearch.dataprepper.pipeline.common.FutureHelper;
import org.opensearch.dataprepper.pipeline.common.TestProcessor;
import org.opensearch.dataprepper.pipeline.router.Router;
import org.opensearch.dataprepper.pipeline.router.RouterGetRecordStrategy;
import org.opensearch.dataprepper.plugins.TestSink;
import org.opensearch.dataprepper.plugins.TestSource;
import org.opensearch.dataprepper.plugins.TestSourceWithCoordination;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.pipeline.router.RouterCopyRecordStrategy;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.sourcecoordination.SourceCoordinatorFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class PipelineTests {
    private static final int TEST_READ_BATCH_TIMEOUT = 3000;
    private static final int TEST_PROCESSOR_THREADS = 1;
    private static final String TEST_PIPELINE_NAME = "test-pipeline";

    private Router router;
    private SourceCoordinatorFactory sourceCoordinatorFactory;
    private Pipeline testPipeline;
    private Duration processorShutdownTimeout;
    private Duration sinkShutdownTimeout;
    private Duration peerForwarderDrainTimeout;
    private EventFactory eventFactory;
    private JacksonEvent event;
    private EventHandle eventHandle;
    private AcknowledgementSetManager acknowledgementSetManager;

    @BeforeEach
    void setup() {
        router = mock(Router.class);
        eventFactory = mock(EventFactory.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        sourceCoordinatorFactory = mock(SourceCoordinatorFactory.class);
        processorShutdownTimeout = Duration.ofSeconds(Math.abs(new Random().nextInt(10)));
        sinkShutdownTimeout = Duration.ofSeconds(Math.abs(new Random().nextInt(10)));
        peerForwarderDrainTimeout = Duration.ofSeconds(Math.abs(new Random().nextInt(10)));
    }

    @AfterEach
    void teardown() {
        if (testPipeline != null && !testPipeline.isStopRequested()) {
            testPipeline.shutdown();
        }
    }

    @Test
    void testPipelineState() {
        final Source<Record<String>> testSource = new TestSource();
        final TestSink testSink = new TestSink();
        final DataFlowComponent<Sink> sinkDataFlowComponent = mock(DataFlowComponent.class);
        when(sinkDataFlowComponent.getComponent()).thenReturn(testSink);
        final Pipeline testPipeline = new Pipeline(TEST_PIPELINE_NAME, testSource, new BlockingBuffer(TEST_PIPELINE_NAME),
                Collections.emptyList(), Collections.singletonList(sinkDataFlowComponent), router, eventFactory, 
                acknowledgementSetManager, sourceCoordinatorFactory, TEST_PROCESSOR_THREADS, TEST_READ_BATCH_TIMEOUT,
                processorShutdownTimeout, sinkShutdownTimeout, peerForwarderDrainTimeout);
        assertThat("Pipeline isStopRequested is expected to be false", testPipeline.isStopRequested(), is(false));
        assertThat("Pipeline is expected to have a default buffer", testPipeline.getBuffer(), notNullValue());
        assertTrue("Pipeline processors should be empty", testPipeline.getProcessorSets().isEmpty());
        testPipeline.execute();
        assertThat("Pipeline isStopRequested is expected to be false", testPipeline.isStopRequested(), is(false));
        testPipeline.shutdown();
        assertThat("Pipeline isStopRequested is expected to be true", testPipeline.isStopRequested(), is(true));
        assertThat("Sink shutdown should be called", testSink.isShutdown, is(true));
    }

    @Test
    void testPipelineStateWithPrepper() {
        final Source<Record<String>> testSource = new TestSource();
        final TestSink testSink = new TestSink();
        final DataFlowComponent<Sink> sinkDataFlowComponent = mock(DataFlowComponent.class);
        when(sinkDataFlowComponent.getComponent()).thenReturn(testSink);
        final TestProcessor testProcessor = new TestProcessor(new PluginSetting("test_processor", new HashMap<>()));
        final Pipeline testPipeline = new Pipeline(TEST_PIPELINE_NAME, testSource, new BlockingBuffer(TEST_PIPELINE_NAME),
                Collections.singletonList(Collections.singletonList(testProcessor)),
                Collections.singletonList(sinkDataFlowComponent),
                router, eventFactory, acknowledgementSetManager, sourceCoordinatorFactory, TEST_PROCESSOR_THREADS,
                TEST_READ_BATCH_TIMEOUT, processorShutdownTimeout, sinkShutdownTimeout, peerForwarderDrainTimeout);
        assertThat("Pipeline isStopRequested is expected to be false", testPipeline.isStopRequested(), is(false));
        assertThat("Pipeline is expected to have a default buffer", testPipeline.getBuffer(), notNullValue());
        assertEquals("Pipeline processorSets size should be 1", 1, testPipeline.getProcessorSets().size());
        testPipeline.execute();
        assertThat("Pipeline isStopRequested is expected to be false", testPipeline.isStopRequested(), is(false));
        testPipeline.shutdown();
        assertThat("Pipeline isStopRequested is expected to be true", testPipeline.isStopRequested(), is(true));
        assertThat("Sink shutdown should be called", testSink.isShutdown, is(true));
        assertThat("Processor shutdown should be called", testProcessor.isShutdown, is(true));
    }

    @Test
    void testPipelineStateWithProcessor() {
        final Source<Record<String>> testSource = new TestSource();
        final TestSink testSink = new TestSink();
        final DataFlowComponent<Sink> sinkDataFlowComponent = mock(DataFlowComponent.class);
        when(sinkDataFlowComponent.getComponent()).thenReturn(testSink);
        final TestProcessor testProcessor = new TestProcessor(new PluginSetting("test_processor", new HashMap<>()));
        final Pipeline testPipeline = new Pipeline(TEST_PIPELINE_NAME, testSource, new BlockingBuffer(TEST_PIPELINE_NAME),
                Collections.singletonList(Collections.singletonList(testProcessor)),
                Collections.singletonList(sinkDataFlowComponent),
                router, eventFactory, acknowledgementSetManager, sourceCoordinatorFactory, TEST_PROCESSOR_THREADS,
                TEST_READ_BATCH_TIMEOUT, processorShutdownTimeout, sinkShutdownTimeout, peerForwarderDrainTimeout);
        assertThat("Pipeline isStopRequested is expected to be false", testPipeline.isStopRequested(), is(false));
        assertThat("Pipeline is expected to have a default buffer", testPipeline.getBuffer(), notNullValue());
        assertEquals("Pipeline processorSets size should be 1", 1, testPipeline.getProcessorSets().size());
        testPipeline.execute();
        assertThat("Pipeline isStopRequested is expected to be false", testPipeline.isStopRequested(), is(false));
        testPipeline.shutdown();
        assertThat("Pipeline isStopRequested is expected to be true", testPipeline.isStopRequested(), is(true));
        assertThat("Sink shutdown should be called", testSink.isShutdown, is(true));
        assertThat("Processor shutdown should be called", testProcessor.isShutdown, is(true));
    }

    @Test
    void testExecuteFailingSource() {
        final Source<Record<String>> testSource = new TestSource(true);
        final TestSink testSink = new TestSink();
        final DataFlowComponent<Sink> sinkDataFlowComponent = mock(DataFlowComponent.class);
        when(sinkDataFlowComponent.getComponent()).thenReturn(testSink);
        try {
            final Pipeline testPipeline = new Pipeline(TEST_PIPELINE_NAME, testSource, new BlockingBuffer(TEST_PIPELINE_NAME),
                    Collections.emptyList(), Collections.singletonList(sinkDataFlowComponent), router,
                    eventFactory, acknowledgementSetManager, sourceCoordinatorFactory, TEST_PROCESSOR_THREADS, TEST_READ_BATCH_TIMEOUT,
                    processorShutdownTimeout, sinkShutdownTimeout, peerForwarderDrainTimeout);
            testPipeline.execute();
        } catch (Exception ex) {
            assertThat("Incorrect exception message", ex.getMessage().contains("Source is expected to fail"));
            assertThat("Exception while starting the source should have pipeline.isStopRequested to false",
                    !testPipeline.isStopRequested());
        }
    }

    @Test
    void testExecuteFailingSink() {
        final Source<Record<String>> testSource = new TestSource();
        final Sink<Record<String>> testSink = new TestSink(true);
        final DataFlowComponent<Sink> sinkDataFlowComponent = mock(DataFlowComponent.class);
        when(sinkDataFlowComponent.getComponent()).thenReturn(testSink);
        try {
            testPipeline = new Pipeline(TEST_PIPELINE_NAME, testSource, new BlockingBuffer(TEST_PIPELINE_NAME),
                    Collections.emptyList(), Collections.singletonList(sinkDataFlowComponent), router,
                    eventFactory, acknowledgementSetManager, sourceCoordinatorFactory, TEST_PROCESSOR_THREADS, TEST_READ_BATCH_TIMEOUT,
                    processorShutdownTimeout, sinkShutdownTimeout, peerForwarderDrainTimeout);
            testPipeline.execute();
            Thread.sleep(TEST_READ_BATCH_TIMEOUT);
        } catch (Exception ex) {
            verifyNoInteractions(sourceCoordinatorFactory);
            assertThat("Incorrect exception message", ex.getMessage().contains("Sink is expected to fail"));
            assertThat("Exception from sink should trigger shutdown of pipeline", testPipeline.isStopRequested());
        }
    }

    @Test
    void testExecuteFailingProcessor() {
        final Source<Record<String>> testSource = new TestSource();
        final Sink<Record<String>> testSink = new TestSink();
        final Processor<Record<String>, Record<String>> testProcessor = new Processor<Record<String>, Record<String>>() {
            @Override
            public Collection<Record<String>> execute(Collection<Record<String>> records) {
                throw new RuntimeException("Processor is expected to fail");
            }

            @Override
            public void prepareForShutdown() {

            }

            @Override
            public boolean isReadyForShutdown() {
                return true;
            }

            @Override
            public void shutdown() {

            }
        };

        final DataFlowComponent<Sink> sinkDataFlowComponent = mock(DataFlowComponent.class);
        when(sinkDataFlowComponent.getComponent()).thenReturn(testSink);
        try {
            testPipeline = new Pipeline(TEST_PIPELINE_NAME, testSource, new BlockingBuffer(TEST_PIPELINE_NAME),
                    Collections.singletonList(Collections.singletonList(testProcessor)), Collections.singletonList(sinkDataFlowComponent),
                    router, eventFactory, acknowledgementSetManager, sourceCoordinatorFactory, TEST_PROCESSOR_THREADS,
                    TEST_READ_BATCH_TIMEOUT, processorShutdownTimeout, sinkShutdownTimeout, peerForwarderDrainTimeout);
            testPipeline.execute();
            Thread.sleep(TEST_READ_BATCH_TIMEOUT);
        } catch (Exception ex) {
            assertThat("Incorrect exception message", ex.getMessage().contains("Processor is expected to fail"));
            assertThat("Exception from processor should trigger shutdown of pipeline", testPipeline.isStopRequested());
        }
    }

    @Test
    void testExecuteOnSourceWithRequiredSourceCoordination_sets_source_coordinator() {
        final Source<Record<String>> testSource = new TestSourceWithCoordination();
        final Source<Record<String>> sourceSpy = spy(testSource);

        final Sink<Record<String>> testSink = new TestSink();
        final DataFlowComponent<Sink> sinkDataFlowComponent = mock(DataFlowComponent.class);

        final SourceCoordinator sourceCoordinator = mock(SourceCoordinator.class);
        given(sourceCoordinatorFactory.provideSourceCoordinator(String.class, UUID.randomUUID().toString())).willReturn(sourceCoordinator);
        when(sinkDataFlowComponent.getComponent()).thenReturn(testSink);
        try {
            testPipeline = new Pipeline(TEST_PIPELINE_NAME, testSource, new BlockingBuffer(TEST_PIPELINE_NAME),
                    Collections.emptyList(), Collections.singletonList(sinkDataFlowComponent), router, eventFactory,
                    acknowledgementSetManager, sourceCoordinatorFactory, TEST_PROCESSOR_THREADS, TEST_READ_BATCH_TIMEOUT,
                    processorShutdownTimeout, sinkShutdownTimeout, peerForwarderDrainTimeout);
            testPipeline.execute();
            Thread.sleep(TEST_READ_BATCH_TIMEOUT);
        } catch (final InterruptedException e) {
            verify((UsesSourceCoordination)sourceSpy).getPartitionProgressStateClass();
            verify((UsesSourceCoordination)sourceSpy).setSourceCoordinator(sourceCoordinator);
        }
    }

    @Test
    void testGetSource() {
        final Source<Record<String>> testSource = new TestSource();
        final TestSink testSink = new TestSink();
        final DataFlowComponent<Sink> sinkDataFlowComponent = mock(DataFlowComponent.class);
        when(sinkDataFlowComponent.getComponent()).thenReturn(testSink);
        final Pipeline testPipeline = new Pipeline(TEST_PIPELINE_NAME, testSource, new BlockingBuffer(TEST_PIPELINE_NAME),
                Collections.emptyList(), Collections.singletonList(sinkDataFlowComponent), router,
                eventFactory, acknowledgementSetManager, sourceCoordinatorFactory, TEST_PROCESSOR_THREADS, TEST_READ_BATCH_TIMEOUT,
                processorShutdownTimeout, sinkShutdownTimeout, peerForwarderDrainTimeout);

        assertEquals(testSource, testPipeline.getSource());
    }

    @Test
    void testGetSinks() {
        final Source<Record<String>> testSource = new TestSource();
        final TestSink testSink = new TestSink();
        final DataFlowComponent<Sink> sinkDataFlowComponent = mock(DataFlowComponent.class);
        when(sinkDataFlowComponent.getComponent()).thenReturn(testSink);
        final Pipeline testPipeline = new Pipeline(TEST_PIPELINE_NAME, testSource, new BlockingBuffer(TEST_PIPELINE_NAME),
                Collections.emptyList(), Collections.singletonList(sinkDataFlowComponent),
                router, eventFactory, acknowledgementSetManager, sourceCoordinatorFactory, TEST_PROCESSOR_THREADS,
                TEST_READ_BATCH_TIMEOUT, processorShutdownTimeout, sinkShutdownTimeout, peerForwarderDrainTimeout);

        assertEquals(1, testPipeline.getSinks().size());
        assertEquals(testSink, testPipeline.getSinks().iterator().next());
    }

    @Nested
    class PublishToSink {

        private List<Sink> sinks;
        private List<Record> records;
        private List<DataFlowComponent<Sink>> dataFlowComponents;
        private Source mockSource;

        @BeforeEach
        void setUp() {
            sinks = IntStream.range(0, 3)
                    .mapToObj(i -> mock(Sink.class))
                    .collect(Collectors.toList());
            mockSource = mock(Source.class);
            when(mockSource.areAcknowledgementsEnabled()).thenReturn(false);

            dataFlowComponents = sinks.stream()
                    .map(sink -> {
                        final DataFlowComponent<Sink> sinkDataFlowComponent = mock(DataFlowComponent.class);
                        when(sinkDataFlowComponent.getComponent()).thenReturn(sink);
                        return sinkDataFlowComponent;
                    })
                    .collect(Collectors.toList());

            records = IntStream.range(0, 100)
                    .mapToObj(i -> mock(Record.class))
                    .collect(Collectors.toList());
        }

        private Pipeline createObjectUnderTest() {
            return new Pipeline(TEST_PIPELINE_NAME, mockSource, mock(Buffer.class), Collections.emptyList(),
                    dataFlowComponents, router, eventFactory, acknowledgementSetManager, sourceCoordinatorFactory, TEST_PROCESSOR_THREADS,
                    TEST_READ_BATCH_TIMEOUT, processorShutdownTimeout, sinkShutdownTimeout, peerForwarderDrainTimeout);
        }

        @Test
        void publishToSinks_calls_route_with_Events_and_Sinks_verify_AcknowledgementSetManager() {

            doAnswer(a -> {
                RouterCopyRecordStrategy routerCopyRecordStrategy = (RouterCopyRecordStrategy)a.getArgument(2);
                Record rec = records.get(0);
                event = mock(JacksonEvent.class);
                eventHandle = mock(EventHandle.class);
                when(event.getEventHandle()).thenReturn(eventHandle);
                when(rec.getData()).thenReturn(event);
                routerCopyRecordStrategy.getRecord(rec);
                routerCopyRecordStrategy.getRecord(rec);
                return null;
            }).when(router)
              .route(anyCollection(), eq(dataFlowComponents), any(RouterGetRecordStrategy.class), any(BiConsumer.class));
            Pipeline pipeline = createObjectUnderTest();
            when(mockSource.areAcknowledgementsEnabled()).thenReturn(true);
            pipeline.publishToSinks(records);
            verify(acknowledgementSetManager).acquireEventReference(any(EventHandle.class));

            verify(router)
                    .route(anyCollection(), eq(dataFlowComponents), any(RouterGetRecordStrategy.class), any(BiConsumer.class));
        }


        @Test
        void publishToSinks_calls_route_with_Events_and_Sinks_verify_InactiveAcknowledgementSetManager() {

            doAnswer(a -> {
                RouterCopyRecordStrategy routerCopyRecordStrategy = (RouterCopyRecordStrategy)a.getArgument(2);
                Record rec = records.get(0);
                event = mock(JacksonEvent.class);
                eventHandle = mock(EventHandle.class);
                when(event.getEventHandle()).thenReturn(null);
                when(rec.getData()).thenReturn(event);
                routerCopyRecordStrategy.getRecord(rec);
                routerCopyRecordStrategy.getRecord(rec);
                return null;
            }).when(router)
              .route(anyCollection(), eq(dataFlowComponents), any(RouterGetRecordStrategy.class), any(BiConsumer.class));
            createObjectUnderTest().publishToSinks(records);
            verifyNoInteractions(acknowledgementSetManager);

            verify(router)
                    .route(anyCollection(), eq(dataFlowComponents), any(RouterGetRecordStrategy.class), any(BiConsumer.class));
        }

        @Test
        void publishToSinks_calls_route_with_Events_and_Sinks() {

            createObjectUnderTest().publishToSinks(records);

            verify(router)
                    .route(anyCollection(), eq(dataFlowComponents), any(RouterGetRecordStrategy.class), any(BiConsumer.class));
        }

        @Nested
        class WithAllRouted {

            @BeforeEach
            void setUp() {
                doAnswer(a -> {
                    Collection<Record> records = a.getArgument(0);
                    dataFlowComponents
                            .stream()
                            .map(DataFlowComponent::getComponent)
                            .forEach(sink -> a.<BiConsumer<Sink, Collection<Record>>>getArgument(3).accept(sink, records));
                    return a;
                })
                        .when(router)
                        .route(anyCollection(), eq(dataFlowComponents), any(RouterGetRecordStrategy.class), any(BiConsumer.class));
            }

            @Test
            void publishToSinks_returns_a_Future_for_each_Sink() {

                final List<Future<Void>> futures = createObjectUnderTest().publishToSinks(records);

                assertThat(futures, notNullValue());
                assertThat(futures.size(), equalTo(sinks.size()));
                for (Future<Void> future : futures) {
                    assertThat(future, notNullValue());
                }
            }

            @Test
            void publishToSinks_writes_Events_to_Sinks() {
                final List<Future<Void>> futures = createObjectUnderTest().publishToSinks(records);

                FutureHelper.awaitFuturesIndefinitely(futures);

                for (Sink sink : sinks) {
                    verify(sink).output(records);
                }
            }

        }

        @Nested
        class WithOneRouted {

            private Sink routedSink;
            private List<Sink> unroutedSinks;

            @BeforeEach
            void setUp() {

                routedSink = dataFlowComponents.get(1).getComponent();

                doAnswer(a -> {
                    Collection<Record> records = a.getArgument(0);
                    a.<BiConsumer<Sink, Collection<Record>>>getArgument(3).accept(routedSink, records);
                    return a;
                })
                        .when(router)
                        .route(anyCollection(), eq(dataFlowComponents), any(RouterGetRecordStrategy.class), any(BiConsumer.class));

                unroutedSinks = dataFlowComponents
                        .stream()
                        .map(DataFlowComponent::getComponent)
                        .filter(s -> s != routedSink)
                        .collect(Collectors.toList());
            }

            @Test
            void publishToSinks_returns_a_Future_for_each_routed_Sink() {

                final List<Future<Void>> futures = createObjectUnderTest().publishToSinks(records);

                assertThat(futures, notNullValue());
                assertThat(futures.size(), equalTo(1));
                for (Future<Void> future : futures) {
                    assertThat(future, notNullValue());
                }
            }


            @Test
            void publishToSinks_writes_Events_to_Sinks_which_have_the_route() {

                final List<Future<Void>> futures = createObjectUnderTest().publishToSinks(records);

                FutureHelper.awaitFuturesIndefinitely(futures);

                verify(routedSink).output(records);

                for (Sink sink : unroutedSinks) {
                    verify(sink, never()).output(records);
                }
            }
        }
    }

    @Test
    void shutdown_calls_added_PipelineObservers() {
        final Source<Record<String>> testSource = new TestSource();
        final DataFlowComponent<Sink> sinkDataFlowComponent = mock(DataFlowComponent.class);
        final TestSink testSink = new TestSink();
        when(sinkDataFlowComponent.getComponent()).thenReturn(testSink);
        testPipeline = new Pipeline(TEST_PIPELINE_NAME, testSource, new BlockingBuffer(TEST_PIPELINE_NAME),
                Collections.emptyList(), Collections.singletonList(sinkDataFlowComponent), router,
                eventFactory, acknowledgementSetManager, sourceCoordinatorFactory, TEST_PROCESSOR_THREADS, TEST_READ_BATCH_TIMEOUT,
                processorShutdownTimeout, sinkShutdownTimeout, peerForwarderDrainTimeout);

        PipelineObserver pipelineObserver = mock(PipelineObserver.class);
        testPipeline.addShutdownObserver(pipelineObserver);

        testPipeline.execute();

        verifyNoInteractions(pipelineObserver);

        testPipeline.shutdown();

        verify(pipelineObserver).shutdown(testPipeline);
    }

    @Test
    void shutdown_does_not_call_removed_PipelineObservers() {
        final Source<Record<String>> testSource = new TestSource();
        final DataFlowComponent<Sink> sinkDataFlowComponent = mock(DataFlowComponent.class);
        final TestSink testSink = new TestSink();
        when(sinkDataFlowComponent.getComponent()).thenReturn(testSink);
        testPipeline = new Pipeline(TEST_PIPELINE_NAME, testSource, new BlockingBuffer(TEST_PIPELINE_NAME),
                Collections.emptyList(), Collections.singletonList(sinkDataFlowComponent), router,
                eventFactory, acknowledgementSetManager, sourceCoordinatorFactory, TEST_PROCESSOR_THREADS, TEST_READ_BATCH_TIMEOUT,
                processorShutdownTimeout, sinkShutdownTimeout, peerForwarderDrainTimeout);

        PipelineObserver pipelineObserver = mock(PipelineObserver.class);
        testPipeline.addShutdownObserver(pipelineObserver);

        testPipeline.execute();

        testPipeline.removeShutdownObserver(pipelineObserver);

        testPipeline.shutdown();
        verifyNoInteractions(pipelineObserver);
    }
}
