/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.parser.PipelineTransformer;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderServer;
import org.opensearch.dataprepper.pipeline.Pipeline;
import org.opensearch.dataprepper.pipeline.PipelineObserver;
import org.opensearch.dataprepper.pipeline.server.DataPrepperServer;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DataPrepperTests {
    private Map<String, Pipeline> parseConfigurationFixture;
    @Mock
    private PipelineTransformer pipelineTransformer;
    @Mock
    private Pipeline pipeline;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private DataPrepperServer dataPrepperServer;
    @Mock
    private PeerForwarderServer peerForwarderServer;
    @Mock
    private Predicate<Map<String, Pipeline>> shouldShutdownOnPipelineFailurePredicate;

    @BeforeEach
    public void before() {
        parseConfigurationFixture = new HashMap<>();
        parseConfigurationFixture.put("testKey", pipeline);
        lenient().when(pipeline.getName()).thenReturn("testKey");
        lenient().when(pipeline.isReady()).thenReturn(true);

        lenient().when(pipelineTransformer.transformConfiguration())
                .thenReturn(parseConfigurationFixture);
    }

    private DataPrepper createObjectUnderTest() throws NoSuchFieldException, IllegalAccessException {
        final DataPrepper dataPrepper = new DataPrepper(pipelineTransformer, pluginFactory, peerForwarderServer, shouldShutdownOnPipelineFailurePredicate);
        final Field dataPrepperServerField = dataPrepper.getClass().getDeclaredField("dataPrepperServer");
        dataPrepperServerField.setAccessible(true);
        dataPrepperServerField.set(dataPrepper, dataPrepperServer);
        dataPrepperServerField.setAccessible(false);

        return dataPrepper;
    }

    @Test
    public void testGivenValidInputThenInstanceCreation() throws NoSuchFieldException, IllegalAccessException {
        assertThat(
                "Given injected with valid beans a DataPrepper bean should be available",
                createObjectUnderTest(),
                Matchers.is(Matchers.notNullValue()));
    }

    @Test
    public void testGivenInvalidInputThenExceptionThrown() {
        final PipelineTransformer pipelineTransformer = mock(PipelineTransformer.class);

        assertThrows(
                RuntimeException.class,
                () -> new DataPrepper(pipelineTransformer, pluginFactory, peerForwarderServer, shouldShutdownOnPipelineFailurePredicate),
                "Exception should be thrown if pipeline parser has no pipeline configuration");
    }

    @Test
    public void testGivenInstantiatedWithPluginFactoryWhenGetPluginFactoryCalledThenReturnSamePluginFactory() throws NoSuchFieldException, IllegalAccessException {
        assertThat(createObjectUnderTest().getPluginFactory(), Matchers.is(pluginFactory));
    }

    @Test
    public void testGivenValidPipelineParserThenReturnResultOfParseConfiguration() throws NoSuchFieldException, IllegalAccessException {
        assertThat(createObjectUnderTest().getTransformationPipelines(), Matchers.is(parseConfigurationFixture));
    }

    @Test
    public void testGivenValidPipelineParserWhenExecuteThenAllPipelinesExecuteAndServerStartAndReturnTrue() throws NoSuchFieldException, IllegalAccessException {
        assertThat(createObjectUnderTest().execute(), Matchers.is(true));

        verify(pipeline).execute();
        verify(dataPrepperServer).start();
        verify(peerForwarderServer).start();
    }

    @Test
    public void testDataPrepperShutdown() throws NoSuchFieldException, IllegalAccessException {
        createObjectUnderTest().shutdownPipelines();
        verify(pipeline).shutdown();
    }

    @Test
    public void testDataPrepperShutdownPipeline() throws NoSuchFieldException, IllegalAccessException {
        final Pipeline randomPipeline = mock(Pipeline.class);
        lenient().when(randomPipeline.isReady()).thenReturn(true);
        parseConfigurationFixture.put("Random Pipeline", randomPipeline);
        createObjectUnderTest().shutdownPipelines("Random Pipeline");

        verify(randomPipeline).shutdown();
    }

    @Test
    public void testDataPrepperShutdownNonExistentPipelineWithoutException() throws NoSuchFieldException, IllegalAccessException {
        createObjectUnderTest().shutdownPipelines("Missing Pipeline");
    }

    @Test
    public void testShutdownServers() throws NoSuchFieldException, IllegalAccessException {
        createObjectUnderTest().shutdownServers();

        verify(dataPrepperServer).stop();
        verify(peerForwarderServer).stop();
    }
    
    @Test
    public void testGivenEnvVarNotSetThenDefaultServiceNameReturned() {
        final String actual = DataPrepper.getServiceNameForMetrics();

        assertThat(actual, Matchers.is("dataprepper"));
    }

    @Test
    void dataPrepper_shuts_down_when_shouldShutdownOnPipelineFailurePredicate_is_true() throws NoSuchFieldException, IllegalAccessException {
        final DataPrepper objectUnderTest = createObjectUnderTest();
        objectUnderTest.execute();
        final ArgumentCaptor<PipelineObserver> pipelineObserverArgumentCaptor = ArgumentCaptor.forClass(PipelineObserver.class);
        verify(pipeline).addShutdownObserver(pipelineObserverArgumentCaptor.capture());

        final PipelineObserver pipelineObserver = pipelineObserverArgumentCaptor.getValue();
        when(shouldShutdownOnPipelineFailurePredicate.test(anyMap()))
                .thenReturn(true);
        pipelineObserver.shutdown(pipeline);

        verify(dataPrepperServer).stop();
        verify(peerForwarderServer).stop();

        final Map<String, Pipeline> transformationPipelines = objectUnderTest.getTransformationPipelines();
        assertThat(transformationPipelines, notNullValue());
        assertThat(transformationPipelines.size(), equalTo(0));
    }

    @Test
    void dataPrepper_does_not_shut_down_when_shouldShutdownOnPipelineFailurePredicate_is_false() throws NoSuchFieldException, IllegalAccessException {
        final DataPrepper objectUnderTest = createObjectUnderTest();
        objectUnderTest.execute();
        final ArgumentCaptor<PipelineObserver> pipelineObserverArgumentCaptor = ArgumentCaptor.forClass(PipelineObserver.class);
        verify(pipeline).addShutdownObserver(pipelineObserverArgumentCaptor.capture());

        final PipelineObserver pipelineObserver = pipelineObserverArgumentCaptor.getValue();
        when(shouldShutdownOnPipelineFailurePredicate.test(anyMap()))
                .thenReturn(false);
        pipelineObserver.shutdown(pipeline);

        verify(dataPrepperServer, never()).stop();
        verify(peerForwarderServer, never()).stop();

        final Map<String, Pipeline> transformationPipelines = objectUnderTest.getTransformationPipelines();
        assertThat(transformationPipelines, notNullValue());
        assertThat(transformationPipelines.size(), equalTo(0));
    }
}
