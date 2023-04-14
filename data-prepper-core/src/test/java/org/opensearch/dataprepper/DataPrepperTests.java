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
import org.opensearch.dataprepper.parser.PipelineParser;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderServer;
import org.opensearch.dataprepper.pipeline.Pipeline;
import org.opensearch.dataprepper.pipeline.PipelineObserver;
import org.opensearch.dataprepper.pipeline.server.DataPrepperServer;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class DataPrepperTests {
    private Map<String, Pipeline> parseConfigurationFixture;
    @Mock
    private PipelineParser pipelineParser;
    @Mock
    private Pipeline pipeline;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private DataPrepperServer dataPrepperServer;
    @Mock
    private PeerForwarderServer peerForwarderServer;

    @BeforeEach
    public void before() {
        parseConfigurationFixture = new HashMap<>();
        parseConfigurationFixture.put("testKey", pipeline);
        lenient().when(pipeline.getName()).thenReturn("testKey");
        lenient().when(pipeline.isReady()).thenReturn(true);

        lenient().when(pipelineParser.parseConfiguration())
                .thenReturn(parseConfigurationFixture);
    }

    private DataPrepper createObjectUnderTest() throws NoSuchFieldException, IllegalAccessException {
        final DataPrepper dataPrepper = new DataPrepper(pipelineParser, pluginFactory, peerForwarderServer);
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
        final PipelineParser pipelineParser = mock(PipelineParser.class);

        assertThrows(
                RuntimeException.class,
                () -> new DataPrepper(pipelineParser, pluginFactory, peerForwarderServer),
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
    void dataPrepper_shuts_down_when_last_pipeline_is_shutdown() throws NoSuchFieldException, IllegalAccessException {
        final DataPrepper objectUnderTest = createObjectUnderTest();
        objectUnderTest.execute();
        final ArgumentCaptor<PipelineObserver> pipelineObserverArgumentCaptor = ArgumentCaptor.forClass(PipelineObserver.class);
        verify(pipeline).addShutdownObserver(pipelineObserverArgumentCaptor.capture());

        final PipelineObserver pipelineObserver = pipelineObserverArgumentCaptor.getValue();
        pipelineObserver.shutdown(pipeline);

        verify(dataPrepperServer).stop();
        verify(peerForwarderServer).stop();

        final Map<String, Pipeline> transformationPipelines = objectUnderTest.getTransformationPipelines();
        assertThat(transformationPipelines, notNullValue());
        assertThat(transformationPipelines.size(), equalTo(0));
    }
}
