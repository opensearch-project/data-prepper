/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper;

import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.parser.PipelineParser;
import com.amazon.dataprepper.pipeline.Pipeline;
import com.amazon.dataprepper.pipeline.server.DataPrepperServer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DataPrepperTests {
    private static Map<String, Pipeline> parseConfigurationFixture;
    private static PipelineParser pipelineParser;
    private static Pipeline pipeline;

    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private DataPrepperServer dataPrepperServer;
    @InjectMocks
    private DataPrepper dataPrepper;

    @BeforeAll
    public static void beforeAll() throws NoSuchFieldException {
        pipelineParser = mock(PipelineParser.class);
        pipeline = mock(Pipeline.class);

        parseConfigurationFixture = new HashMap<>();
        parseConfigurationFixture.put("testKey", pipeline);

        when(pipelineParser.parseConfiguration())
                .thenReturn(parseConfigurationFixture);
    }

    @BeforeEach
    public void before() throws NoSuchFieldException, IllegalAccessException {
        // Use reflection to set dataPrepper.dataPrepperServer because @InjectMock will not use field injection.
        final Field dataPrepperServerField = dataPrepper.getClass().getDeclaredField("dataPrepperServer");
        dataPrepperServerField.setAccessible(true);
        dataPrepperServerField.set(dataPrepper, dataPrepperServer);
        dataPrepperServerField.setAccessible(false);
    }

    @Test
    public void testGivenValidInputThenInstanceCreation() {
        assertThat(
                "Given injected with valid beans a Data Prepper bean should be available",
                dataPrepper,
                Matchers.is(Matchers.notNullValue()));
    }

    @Test
    public void testGivenInvalidInputThenExceptionThrown() {
        PipelineParser pipelineParser = mock(PipelineParser.class);

        assertThrows(
                RuntimeException.class,
                () -> new DataPrepper(pipelineParser, pluginFactory),
                "Exception should be thrown if pipeline parser has no pipeline configuration");
    }

    @Test
    public void testGivenInstantiatedWithPluginFactoryWhenGetPluginFactoryCalledThenReturnSamePluginFactory() {
        assertThat(dataPrepper.getPluginFactory(), Matchers.is(pluginFactory));
    }

    @Test
    public void testGivenValidPipelineParserThenReturnResultOfParseConfiguration() {
        assertThat(dataPrepper.getTransformationPipelines(), Matchers.is(parseConfigurationFixture));
    }

    @Test
    public void testGivenValidPipelineParserWhenExecuteThenAllPipelinesExecuteAndServerStartAndReturnTrue() {
        assertThat(dataPrepper.execute(), Matchers.is(true));

        verify(pipeline, times(1)).execute();
        verify(dataPrepperServer, times(1)).start();
    }

    @Test
    public void testDataPrepperShutdown() {
        dataPrepper.shutdown();
        verify(pipeline, times(1)).shutdown();
    }

    @Test
    public void testDataPrepperShutdownPipeline() {
        Pipeline randomPipeline = mock(Pipeline.class);
        parseConfigurationFixture.put("Random Pipeline", randomPipeline);
        dataPrepper.shutdown("Random Pipeline");

        verify(randomPipeline, times(1)).shutdown();
    }

    @Test
    public void testDataPrepperShutdownNonExistentPipelineWithoutException() {
        dataPrepper.shutdown("Missing Pipeline");
    }

    @Test
    public void testShutdownDataPrepperServer() {
        dataPrepper.shutdownDataPrepperServer();

        verify(dataPrepperServer, times(1)).stop();
    }
    
    @Test
    public void foo() throws NoSuchFieldException, IllegalAccessException {
        final Field dataPrepperServerField = DataPrepper.class.getDeclaredField("DEFAULT_SERVICE_NAME");
        dataPrepperServerField.setAccessible(true);
        String expected = (String) dataPrepperServerField.get(null);
        dataPrepperServerField.setAccessible(false);

        String actual = DataPrepper.getServiceNameForMetrics();

        assertThat(actual, Matchers.is(expected));
    }
}
