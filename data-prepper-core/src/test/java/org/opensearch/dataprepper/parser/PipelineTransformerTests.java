/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.TestDataProvider;
import org.opensearch.dataprepper.acknowledgements.DefaultAcknowledgementSetManager;
import org.opensearch.dataprepper.breaker.CircuitBreakerManager;
import org.opensearch.dataprepper.core.event.EventFactoryApplicationContextMarker;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderProvider;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderReceiveBuffer;
import org.opensearch.dataprepper.pipeline.Pipeline;
import org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationFileReader;
import org.opensearch.dataprepper.pipeline.parser.PipelinesDataflowModelParser;
import org.opensearch.dataprepper.pipeline.router.RouterFactory;
import org.opensearch.dataprepper.plugin.DefaultPluginFactory;
import org.opensearch.dataprepper.sourcecoordination.SourceCoordinatorFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PipelineTransformerTests {
    private PeerForwarderProvider peerForwarderProvider;

    @Mock
    private PipelinesDataFlowModel pipelinesDataFlowModel;
    @Mock
    private RouterFactory routerFactory;

    @Mock
    private DataPrepperConfiguration dataPrepperConfiguration;
    @Mock
    private PeerForwarderConfiguration peerForwarderConfiguration;
    @Mock
    private PeerForwarderReceiveBuffer buffer;

    @Mock
    private SourceCoordinatorFactory sourceCoordinatorFactory;
    @Mock
    private CircuitBreakerManager circuitBreakerManager;

    private EventFactory eventFactory;

    private DefaultAcknowledgementSetManager acknowledgementSetManager;

    private PluginFactory pluginFactory;

    @BeforeEach
    void setUp() {
        peerForwarderProvider = mock(PeerForwarderProvider.class);
        eventFactory = mock(EventFactory.class);
        acknowledgementSetManager = mock(DefaultAcknowledgementSetManager.class);
        final AnnotationConfigApplicationContext publicContext = new AnnotationConfigApplicationContext();
        publicContext.refresh();

        final AnnotationConfigApplicationContext coreContext = new AnnotationConfigApplicationContext();
        coreContext.setParent(publicContext);

        coreContext.scan(EventFactoryApplicationContextMarker.class.getPackage().getName());

        coreContext.scan(DefaultAcknowledgementSetManager.class.getPackage().getName());

        coreContext.scan(DefaultPluginFactory.class.getPackage().getName());
        coreContext.registerBean(DataPrepperConfiguration.class, () -> dataPrepperConfiguration);
        coreContext.registerBean(PipelinesDataFlowModel.class, () -> pipelinesDataFlowModel);
        coreContext.refresh();
        pluginFactory = coreContext.getBean(DefaultPluginFactory.class);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(dataPrepperConfiguration);
    }

    private PipelineTransformer createObjectUnderTest(final String pipelineConfigurationFileLocation) {

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataflowModelParser(
                new PipelineConfigurationFileReader(pipelineConfigurationFileLocation)).parseConfiguration();
        return new PipelineTransformer(pipelinesDataFlowModel, pluginFactory, peerForwarderProvider,
                                  routerFactory, dataPrepperConfiguration, circuitBreakerManager, eventFactory,
                                  acknowledgementSetManager, sourceCoordinatorFactory);
    }

    @Test
    void parseConfiguration_with_multiple_valid_pipelines_creates_the_correct_pipelineMap() {
        mockDataPrepperConfigurationAccesses();
        final PipelineTransformer pipelineTransformer =
                createObjectUnderTest(TestDataProvider.VALID_MULTIPLE_PIPELINE_CONFIG_FILE);
        final Map<String, Pipeline> actualPipelineMap = pipelineTransformer.transformConfiguration();
        assertThat(actualPipelineMap.keySet(), equalTo(TestDataProvider.VALID_MULTIPLE_PIPELINE_NAMES));
        verifyDataPrepperConfigurationAccesses(actualPipelineMap.keySet().size());
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void parseConfiguration_with_invalid_root_pipeline_creates_empty_pipelinesMap() {
        final PipelineTransformer pipelineTransformer =
                createObjectUnderTest(TestDataProvider.CONNECTED_PIPELINE_ROOT_SOURCE_INCORRECT);
        final Map<String, Pipeline> connectedPipelines = pipelineTransformer.transformConfiguration();
        assertThat(connectedPipelines.size(), equalTo(0));
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void parseConfiguration_with_incorrect_child_pipeline_returns_empty_pipelinesMap() {
        mockDataPrepperConfigurationAccesses();
        final PipelineTransformer pipelineTransformer =
                createObjectUnderTest(TestDataProvider.CONNECTED_PIPELINE_CHILD_PIPELINE_INCORRECT);
        final Map<String, Pipeline> connectedPipelines = pipelineTransformer.transformConfiguration();
        assertThat(connectedPipelines.size(), equalTo(0));
        verifyDataPrepperConfigurationAccesses();
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void parseConfiguration_with_a_single_pipeline_with_empty_source_settings_returns_that_pipeline() {
        mockDataPrepperConfigurationAccesses();
        final PipelineTransformer pipelineTransformer =
                createObjectUnderTest(TestDataProvider.VALID_SINGLE_PIPELINE_EMPTY_SOURCE_PLUGIN_FILE);
        final Map<String, Pipeline> actualPipelineMap = pipelineTransformer.transformConfiguration();
        assertThat(actualPipelineMap.keySet().size(), equalTo(1));
        verifyDataPrepperConfigurationAccesses();
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void parseConfiguration_with_cycles_in_multiple_pipelines_should_throw() {
        final PipelineTransformer pipelineTransformer =
                createObjectUnderTest(TestDataProvider.CYCLE_MULTIPLE_PIPELINE_CONFIG_FILE);

        final RuntimeException actualException = assertThrows(RuntimeException.class, pipelineTransformer::transformConfiguration);
        assertThat(actualException.getMessage(),
                equalTo("Provided configuration results in a loop, check pipeline: test-pipeline-1"));
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void parseConfiguration_with_incorrect_source_mapping_in_multiple_pipelines_should_throw() {
        final PipelineTransformer pipelineTransformer =
                createObjectUnderTest(TestDataProvider.INCORRECT_SOURCE_MULTIPLE_PIPELINE_CONFIG_FILE);

        final RuntimeException actualException = assertThrows(RuntimeException.class, pipelineTransformer::transformConfiguration);
        assertThat(actualException.getMessage(),
                equalTo("Invalid configuration, expected source test-pipeline-1 for pipeline test-pipeline-2 is missing"));
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void parseConfiguration_with_compatible_version() {
        final PipelineTransformer pipelineTransformer =
            createObjectUnderTest(TestDataProvider.COMPATIBLE_VERSION_CONFIG_FILE);
        final Map<String, Pipeline> connectedPipelines = pipelineTransformer.transformConfiguration();
        assertThat(connectedPipelines.size(), equalTo(1));
        verify(dataPrepperConfiguration).getProcessorShutdownTimeout();
        verify(dataPrepperConfiguration).getSinkShutdownTimeout();
        verify(dataPrepperConfiguration).getPeerForwarderConfiguration();
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void parseConfiguration_with_missing_pipeline_name_should_throw() {
        final PipelineTransformer pipelineTransformer =
                createObjectUnderTest(TestDataProvider.MISSING_NAME_MULTIPLE_PIPELINE_CONFIG_FILE);

        final RuntimeException actualException = assertThrows(RuntimeException.class, pipelineTransformer::transformConfiguration);
        assertThat(actualException.getMessage(),
                equalTo("name is a required attribute for sink pipeline plugin, " +
                    "check pipeline: test-pipeline-1"));
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void parseConfiguration_with_missing_pipeline_name_in_multiple_pipelines_should_throw() {
        final PipelineTransformer pipelineTransformer =
                createObjectUnderTest(TestDataProvider.MISSING_PIPELINE_MULTIPLE_PIPELINE_CONFIG_FILE);
        final RuntimeException actualException = assertThrows(RuntimeException.class, pipelineTransformer::transformConfiguration);
        assertThat(actualException.getMessage(), equalTo("Invalid configuration, no pipeline is defined with name test-pipeline-4"));
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void testMultipleSinks() {
        mockDataPrepperConfigurationAccesses();
        final PipelineTransformer pipelineTransformer =
                createObjectUnderTest(TestDataProvider.VALID_MULTIPLE_SINKS_CONFIG_FILE);
        final Map<String, Pipeline> pipelineMap = pipelineTransformer.transformConfiguration();
        assertThat(pipelineMap.keySet().size(), equalTo(3));
        verifyDataPrepperConfigurationAccesses(pipelineMap.keySet().size());
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void testMultipleProcessors() {
        mockDataPrepperConfigurationAccesses();
        final PipelineTransformer pipelineTransformer =
                createObjectUnderTest(TestDataProvider.VALID_MULTIPLE_PROCESSERS_CONFIG_FILE);
        final Map<String, Pipeline> pipelineMap = pipelineTransformer.transformConfiguration();
        assertThat(pipelineMap.keySet().size(), equalTo(3));
        verifyDataPrepperConfigurationAccesses(pipelineMap.keySet().size());
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void parseConfiguration_with_routes_creates_correct_pipeline() {
        mockDataPrepperConfigurationAccesses();
        final PipelineTransformer pipelineTransformer =
                createObjectUnderTest("src/test/resources/valid_multiple_sinks_with_routes.yml");
        final Map<String, Pipeline> pipelineMap = pipelineTransformer.transformConfiguration();
        assertThat(pipelineMap.keySet().size(), equalTo(3));
        verifyDataPrepperConfigurationAccesses(pipelineMap.keySet().size());

        final Pipeline entryPipeline = pipelineMap.get("entry-pipeline");
        assertThat(entryPipeline, notNullValue());
        assertThat(entryPipeline.getSinks(), notNullValue());
        assertThat(entryPipeline.getSinks().size(), equalTo(2));
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void getPeerForwarderDrainDuration_peerForwarderConfigurationNotSet() {
        when(dataPrepperConfiguration.getPeerForwarderConfiguration()).thenReturn(null);

        final PipelineTransformer pipelineTransformer = createObjectUnderTest(TestDataProvider.VALID_MULTIPLE_PIPELINE_CONFIG_FILE);
        final Duration result = pipelineTransformer.getPeerForwarderDrainTimeout(dataPrepperConfiguration);
        assertThat(result, is(Duration.ofSeconds(0)));

        verify(dataPrepperConfiguration).getPeerForwarderConfiguration();
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void getPeerForwarderDrainDuration_IsSet() {
        final Duration expectedResult = Duration.ofSeconds(Math.abs(new Random().nextInt()));
        when(dataPrepperConfiguration.getPeerForwarderConfiguration()).thenReturn(peerForwarderConfiguration);
        when(peerForwarderConfiguration.getDrainTimeout()).thenReturn(expectedResult);

        final PipelineTransformer pipelineTransformer = createObjectUnderTest(TestDataProvider.VALID_MULTIPLE_PIPELINE_CONFIG_FILE);
        final Duration result = pipelineTransformer.getPeerForwarderDrainTimeout(dataPrepperConfiguration);
        assertThat(result, is(expectedResult));

        verify(dataPrepperConfiguration).getPeerForwarderConfiguration();
        verify(peerForwarderConfiguration).getDrainTimeout();
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void parseConfiguration_uses_CircuitBreaking_buffer_when_circuit_breakers_applied() {
        final CircuitBreaker circuitBreaker = mock(CircuitBreaker.class);
        when(circuitBreakerManager.getGlobalCircuitBreaker())
                .thenReturn(Optional.of(circuitBreaker));
        final PipelineTransformer objectUnderTest =
                createObjectUnderTest(TestDataProvider.VALID_SINGLE_PIPELINE_EMPTY_SOURCE_PLUGIN_FILE);

        final Map<String, Pipeline> pipelineMap = objectUnderTest.transformConfiguration();

        assertThat(pipelineMap.size(), equalTo(1));
        assertThat(pipelineMap, hasKey("test-pipeline-1"));
        final Pipeline pipeline = pipelineMap.get("test-pipeline-1");
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getBuffer(), instanceOf(CircuitBreakingBuffer.class));

        verify(dataPrepperConfiguration).getProcessorShutdownTimeout();
        verify(dataPrepperConfiguration).getSinkShutdownTimeout();
        verify(dataPrepperConfiguration).getPeerForwarderConfiguration();
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void parseConfiguration_uses_unwrapped_buffer_when_circuit_breakers_applied_but_Buffer_is_off_heap() {
        final PipelineTransformer objectUnderTest =
                createObjectUnderTest(TestDataProvider.VALID_OFF_HEAP_FILE);

        final Map<String, Pipeline> pipelineMap = objectUnderTest.transformConfiguration();

        assertThat(pipelineMap.size(), equalTo(1));
        assertThat(pipelineMap, hasKey("test-pipeline-1"));
        final Pipeline pipeline = pipelineMap.get("test-pipeline-1");
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getBuffer(), notNullValue());
        assertThat(pipeline.getBuffer(), CoreMatchers.not(instanceOf(CircuitBreakingBuffer.class)));

        verifyNoInteractions(circuitBreakerManager);
        verify(dataPrepperConfiguration).getProcessorShutdownTimeout();
        verify(dataPrepperConfiguration).getSinkShutdownTimeout();
        verify(dataPrepperConfiguration).getPeerForwarderConfiguration();
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void parseConfiguration_uses_unwrapped_buffer_when_no_circuit_breakers_are_applied() {
        when(circuitBreakerManager.getGlobalCircuitBreaker())
                .thenReturn(Optional.empty());
        final PipelineTransformer objectUnderTest =
                createObjectUnderTest(TestDataProvider.VALID_SINGLE_PIPELINE_EMPTY_SOURCE_PLUGIN_FILE);

        final Map<String, Pipeline> pipelineMap = objectUnderTest.transformConfiguration();

        assertThat(pipelineMap.size(), equalTo(1));
        assertThat(pipelineMap, hasKey("test-pipeline-1"));
        final Pipeline pipeline = pipelineMap.get("test-pipeline-1");
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getBuffer(), notNullValue());
        assertThat(pipeline.getBuffer(), CoreMatchers.not(instanceOf(CircuitBreakingBuffer.class)));

        verify(dataPrepperConfiguration).getProcessorShutdownTimeout();
        verify(dataPrepperConfiguration).getSinkShutdownTimeout();
        verify(dataPrepperConfiguration).getPeerForwarderConfiguration();
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    @Test
    void parseConfiguration_uses_unwrapped_buffer_for_pipeline_connectors() {
        final CircuitBreaker circuitBreaker = mock(CircuitBreaker.class);
        when(circuitBreakerManager.getGlobalCircuitBreaker())
                .thenReturn(Optional.of(circuitBreaker));
        final PipelineTransformer objectUnderTest =
                createObjectUnderTest(TestDataProvider.VALID_MULTIPLE_PIPELINE_CONFIG_FILE);

        final Map<String, Pipeline> pipelineMap = objectUnderTest.transformConfiguration();

        assertThat(pipelineMap, hasKey("test-pipeline-1"));
        final Pipeline entryPipeline = pipelineMap.get("test-pipeline-1");
        assertThat(entryPipeline, notNullValue());
        assertThat(entryPipeline.getBuffer(), instanceOf(CircuitBreakingBuffer.class));

        assertThat(pipelineMap, hasKey("test-pipeline-2"));
        final Pipeline connectedPipeline = pipelineMap.get("test-pipeline-2");
        assertThat(connectedPipeline, notNullValue());
        assertThat(connectedPipeline.getBuffer(), notNullValue());
        assertThat(connectedPipeline.getBuffer(), CoreMatchers.not(instanceOf(CircuitBreakingBuffer.class)));

        verify(dataPrepperConfiguration, times(3)).getProcessorShutdownTimeout();
        verify(dataPrepperConfiguration, times(3)).getSinkShutdownTimeout();
        verify(dataPrepperConfiguration, times(3)).getPeerForwarderConfiguration();
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    private void mockDataPrepperConfigurationAccesses() {
        when(dataPrepperConfiguration.getProcessorShutdownTimeout()).thenReturn(Duration.ofSeconds(Math.abs(new Random().nextInt())));
        when(dataPrepperConfiguration.getSinkShutdownTimeout()).thenReturn(Duration.ofSeconds(Math.abs(new Random().nextInt())));
        when(dataPrepperConfiguration.getPeerForwarderConfiguration()).thenReturn(peerForwarderConfiguration);
        when(peerForwarderConfiguration.getDrainTimeout()).thenReturn(Duration.ofSeconds(Math.abs(new Random().nextInt())));
    }

    private void verifyDataPrepperConfigurationAccesses() {
        verifyDataPrepperConfigurationAccesses(1);
    }

    private void verifyDataPrepperConfigurationAccesses(final int times) {
        verify(dataPrepperConfiguration, times(times)).getProcessorShutdownTimeout();
        verify(dataPrepperConfiguration, times(times)).getSinkShutdownTimeout();
        verify(dataPrepperConfiguration, times(times)).getPeerForwarderConfiguration();
        verify(peerForwarderConfiguration, times(times)).getDrainTimeout();
    }

    @ParameterizedTest
    @MethodSource("provideGetSecondaryBufferArgs")
    void getSecondaryBuffers(final int outerMapEntryCount, final int innerMapEntryCount) {
        final Map<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> bufferMap = generateBufferMap(outerMapEntryCount, innerMapEntryCount);
        when(peerForwarderProvider.getPipelinePeerForwarderReceiveBufferMap()).thenReturn(bufferMap);

        final PipelineTransformer pipelineTransformer = createObjectUnderTest(TestDataProvider.VALID_MULTIPLE_PIPELINE_CONFIG_FILE);
        final List<Buffer> secondaryBuffers = pipelineTransformer.getSecondaryBuffers();
        assertThat(secondaryBuffers.size(), is(outerMapEntryCount * innerMapEntryCount));
        secondaryBuffers.forEach(retrievedBuffer -> assertThat(retrievedBuffer, is(buffer)));

        verify(peerForwarderProvider).getPipelinePeerForwarderReceiveBufferMap();
        verify(dataPrepperConfiguration).getPipelineExtensions();
    }

    private static Stream<Arguments> provideGetSecondaryBufferArgs() {
        return Stream.of(
                Arguments.of(0, 0),
                Arguments.of(0, 1),
                Arguments.of(0, 2),
                Arguments.of(0, 3),
                Arguments.of(1, 0),
                Arguments.of(1, 1),
                Arguments.of(1, 2),
                Arguments.of(1, 3),
                Arguments.of(2, 2),
                Arguments.of(new Random().nextInt(5) + 3, new Random().nextInt(5) + 1)
        );
    }

    private Map<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> generateBufferMap(final int outerMapEntryCount, final int innerMapEntryCount) {
        final Map<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> bufferMap = new HashMap<>();
        for (int i = 0; i < outerMapEntryCount; i++) {
            final Map<String, PeerForwarderReceiveBuffer<Record<Event>>> innerMap = new HashMap<>();
            for (int j = 0; j < innerMapEntryCount; j++) {
                innerMap.put(UUID.randomUUID().toString(), buffer);
            }

            bufferMap.put(UUID.randomUUID().toString(), innerMap);
        }

        return bufferMap;
    }
}
