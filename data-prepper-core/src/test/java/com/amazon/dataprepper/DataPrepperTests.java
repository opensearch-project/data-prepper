package com.amazon.dataprepper;

import io.micrometer.core.instrument.Measurement;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.Permission;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.amazon.dataprepper.TestDataProvider.NO_PIPELINES_EXECUTE_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_MULTIPLE_PIPELINE_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_MULTIPLE_PIPELINE_NAMES;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class DataPrepperTests {

    private SecurityManager actualSecurityManager;

    @Before
    public void setup() throws Exception {
        actualSecurityManager = System.getSecurityManager();
        System.setSecurityManager(new CustomSecurityManager());
        DataPrepper.configure(TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE);
    }

    @After
    public void teardown() {
        System.setSecurityManager(actualSecurityManager);
    }

    @Test
    public void testInstanceCreation() {
        DataPrepper testDataPrepper1 = DataPrepper.getInstance();
        assertThat("Failed to retrieve a valid Data Prepper instance", testDataPrepper1, is(notNullValue()));
        DataPrepper testDataPrepper2 = DataPrepper.getInstance();
        assertThat("Data Prepper has to be singleton", testDataPrepper2, is(testDataPrepper1));
    }

    @Test
    public void testDataPrepperSysMetrics() {
        // Test retrieve gauge in ClassLoaderMetrics
        final List<Measurement> classesLoaded = getSysMeasurementList("jvm.classes.loaded");
        Assert.assertEquals(1, classesLoaded.size());
        // Test retrieve gauge in JvmMemoryMetrics
        final List<Measurement> jvmBufferCount = getSysMeasurementList("jvm.buffer.count");
        Assert.assertEquals(1, jvmBufferCount.size());
        // Test retrieve gauge in JvmGcMetrics
        final List<Measurement> jvmGcMaxDataSize = getSysMeasurementList("jvm.gc.max.data.size");
        Assert.assertEquals(1, jvmGcMaxDataSize.size());
        // Test retrieve gauge in ProcessorMetrics
        final List<Measurement> sysCPUCount = getSysMeasurementList("system.cpu.count");
        Assert.assertEquals(1, sysCPUCount.size());
        // Test retrieve gauge in JvmThreadMetrics
        final List<Measurement> jvmThreadsPeak = getSysMeasurementList("jvm.threads.peak");
        Assert.assertEquals(1, jvmThreadsPeak.size());
    }

    @Test
    public void testCustomConfiguration() {
        DataPrepper testInstance = DataPrepper.getInstance();
        Assert.assertEquals(5678, DataPrepper.getConfiguration().getServerPort());
    }

    @Test
    public void testNoPipelinesToExecute() {
        try {
            DataPrepper testDataPrepper = DataPrepper.getInstance();
            testDataPrepper.execute(NO_PIPELINES_EXECUTE_CONFIG_FILE);
        } catch (SystemExitException ex) {
            assertThat("Data Prepper should exit with status 1", ex.getExitStatus(), is(1));
            assertThat("Data Prepper exit message is incorrect",
                    ex.getMessage().contains("System exit was initiated"));
        }
    }

    @Test
    public void testDataPrepperExecuteAndShutdown() {
        final DataPrepper testDataPrepper = DataPrepper.getInstance();
        boolean executionStatus = testDataPrepper.execute(VALID_MULTIPLE_PIPELINE_CONFIG_FILE);
        assertThat("Failed to initiate execution", executionStatus);
        testDataPrepper.shutdown();
        //call shutdown() twice to ensure nothing breaks
        testDataPrepper.shutdown();
        VALID_MULTIPLE_PIPELINE_NAMES.forEach(testDataPrepper::shutdown);
        testDataPrepper.shutdown("pipeline_does_not_exist"); //this does nothing
    }


    public static class CustomSecurityManager extends SecurityManager {
        @Override
        public void checkPermission(Permission perm) {
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
            // allow anything.
        }

        @Override
        public void checkExit(int status) {
            super.checkExit(status);
            throw new SystemExitException(status);
        }
    }

    public static class SystemExitException extends SecurityException {
        private final int status;

        public SystemExitException(int status) {
            super("System exit was initiated");
            this.status = status;
        }

        public int getExitStatus() {
            return this.status;
        }
    }

    private static List<Measurement> getSysMeasurementList(final String meterName) {
        return StreamSupport.stream(DataPrepper.getSysJVMMeterRegistry().find(meterName).meter().measure().spliterator(), false)
                .collect(Collectors.toList());
    }
}
