/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper;

import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.parser.PipelineParser;
import com.amazon.dataprepper.parser.config.DataPrepperArgs;
import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.amazon.dataprepper.pipeline.Pipeline;
import com.amazon.dataprepper.pipeline.server.DataPrepperServer;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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

    @Mock
    private Pipeline pipeline;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private DataPrepperConfiguration configuration;
    @Mock
    private DataPrepperServer dataPrepperServer;
    @InjectMocks
    private DataPrepper dataPrepper;

    @BeforeAll
    public static void beforeAll() {
        pipelineParser = mock(PipelineParser.class);

        parseConfigurationFixture = new HashMap<>();
        parseConfigurationFixture.put("testKey", mock(Pipeline.class));

        when(pipelineParser.parseConfiguration())
                .thenReturn(parseConfigurationFixture);
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
                () -> new DataPrepper(configuration, pipelineParser, pluginFactory),
                "Exception should be thrown if pipeline parser has no pipeline configuration");
    }

    @Test
    public void testGivenInstantiatedWithPluginFactoryWhenGetPluginFactoryCalledThenReturnSamePluginFactory() {
        assertThat(dataPrepper.getPluginFactory(), Matchers.is(pluginFactory));
    }

    @Test
    public void testGivenValidPipelineParserWhenExecuteThenAllPipelinesExecuteAndServerStartAndReturnTrue() {
        assertThat(dataPrepper.execute(), Matchers.is(true));

        verify(pipeline, times(1)).execute();
        verify(dataPrepperServer, times(1)).start();
    }

//    @Before
//    public void setup() throws Exception {
//        actualSecurityManager = System.getSecurityManager();
//        System.setSecurityManager(new CustomSecurityManager());
//        DataPrepper.configure(TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE);
//    }
//
//    @After
//    public void teardown() {
//        System.setSecurityManager(actualSecurityManager);
//    }
//
//    @Test
//    public void testInstanceCreation() {
//        DataPrepper testDataPrepper1 = DataPrepper.getInstance();
//        assertThat("Failed to retrieve a valid Data Prepper instance", testDataPrepper1, is(notNullValue()));
//        DataPrepper testDataPrepper2 = DataPrepper.getInstance();
//        assertThat("Data Prepper has to be singleton", testDataPrepper2, is(testDataPrepper1));
//    }
//
//    @Test
//    public void testDataPrepperSystemMetrics() {
//        // Test retrieve gauge in ClassLoaderMetrics
//        final List<Measurement> classesLoaded = getSystemMeasurementList("jvm.classes.loaded");
//        Assert.assertEquals(1, classesLoaded.size());
//        // Test retrieve gauge in JvmMemoryMetrics
//        final List<Measurement> jvmBufferCount = getSystemMeasurementList("jvm.buffer.count");
//        Assert.assertEquals(1, jvmBufferCount.size());
//        // Test retrieve gauge in JvmGcMetrics
//        final List<Measurement> jvmGcMaxDataSize = getSystemMeasurementList("jvm.gc.max.data.size");
//        Assert.assertEquals(1, jvmGcMaxDataSize.size());
//        // Test retrieve gauge in ProcessorMetrics
//        final List<Measurement> sysCPUCount = getSystemMeasurementList("system.cpu.count");
//        Assert.assertEquals(1, sysCPUCount.size());
//        // Test retrieve gauge in JvmThreadMetrics
//        final List<Measurement> jvmThreadsPeak = getSystemMeasurementList("jvm.threads.peak");
//        Assert.assertEquals(1, jvmThreadsPeak.size());
//    }
//
//    @Test
//    public void testCustomConfiguration() {
//        DataPrepper testInstance = DataPrepper.getInstance();
//        Assert.assertEquals(5678, DataPrepper.getConfiguration().getServerPort());
//    }
//
//    @Test
//    public void testNoPipelinesToExecute() {
//        try {
//            DataPrepper testDataPrepper = DataPrepper.getInstance();
//            testDataPrepper.execute(NO_PIPELINES_EXECUTE_CONFIG_FILE);
//        } catch (SystemExitException ex) {
//            assertThat("Data Prepper should exit with status 1", ex.getExitStatus(), is(1));
//            assertThat("Data Prepper exit message is incorrect",
//                    ex.getMessage().contains("System exit was initiated"));
//        }
//    }
//
//    @Test
//    public void testDataPrepperExecuteAndShutdown() {
//        final DataPrepper testDataPrepper = DataPrepper.getInstance();
//        boolean executionStatus = testDataPrepper.execute(VALID_MULTIPLE_PIPELINE_CONFIG_FILE);
//        assertThat("Failed to initiate execution", executionStatus);
//        testDataPrepper.shutdown();
//        //call shutdown() twice to ensure nothing breaks
//        testDataPrepper.shutdown();
//        VALID_MULTIPLE_PIPELINE_NAMES.forEach(testDataPrepper::shutdown);
//        testDataPrepper.shutdown("pipeline_does_not_exist"); //this does nothing
//    }
//
//
//    public static class CustomSecurityManager extends SecurityManager {
//        @Override
//        public void checkPermission(Permission perm) {
//        }
//
//        @Override
//        public void checkPermission(Permission perm, Object context) {
//            // allow anything.
//        }
//
//        @Override
//        public void checkExit(int status) {
//            super.checkExit(status);
//            throw new SystemExitException(status);
//        }
//    }
//
//    public static class SystemExitException extends SecurityException {
//        private final int status;
//
//        public SystemExitException(int status) {
//            super("System exit was initiated");
//            this.status = status;
//        }
//
//        public int getExitStatus() {
//            return this.status;
//        }
//    }
//
//    private static List<Measurement> getSystemMeasurementList(final String meterName) {
//        return StreamSupport.stream(DataPrepper.getSystemMeterRegistry().find(meterName).meter().measure().spliterator(), false)
//                .collect(Collectors.toList());
//    }
}
