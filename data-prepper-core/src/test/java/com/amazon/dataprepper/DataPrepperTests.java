package com.amazon.dataprepper;

import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.Permission;

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
    public void testCustomConfiguration() {
        DataPrepper.configure(TestDataProvider.VALID_DATA_PREPPER_CONFIG_FILE);
        DataPrepper testInstance = DataPrepper.getInstance();
        Assert.assertEquals(1234, DataPrepper.getConfiguration().getServerPort());
        Assert.assertEquals(Level.DEBUG, DataPrepper.getConfiguration().getLog4JConfiguration().getLevel());
        Assert.assertEquals("file.txt", DataPrepper.getConfiguration().getLog4JConfiguration().getFilePath());
        Assert.assertEquals("1GB", DataPrepper.getConfiguration().getLog4JConfiguration().getMaxFileSize());
        Assert.assertEquals("10", DataPrepper.getConfiguration().getLog4JConfiguration().getMaxBackupIndex());
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
}
