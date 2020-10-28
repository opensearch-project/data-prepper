package com.amazon.situp;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.Permission;

import static com.amazon.situp.TestDataProvider.NO_PIPELINES_EXECUTE_CONFIG_FILE;
import static com.amazon.situp.TestDataProvider.VALID_MULTIPLE_PIPELINE_CONFIG_FILE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class SitupTests {

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
        Situp testSitup1 = Situp.getInstance();
        assertThat("Failed to retrieve a valid situp instance", testSitup1, is(notNullValue()));
        Situp testSitup2 = Situp.getInstance();
        assertThat("Situp has to be singleton", testSitup2, is(testSitup1));
    }

    @Test
    public void testNoPipelinesToExecute() {
        try {
            Situp testSitup = Situp.getInstance();
            testSitup.execute(NO_PIPELINES_EXECUTE_CONFIG_FILE);
        } catch (SystemExitException ex) {
            assertThat("Situp should exit with status 1", ex.getExitStatus(), is(1));
            assertThat("Situp exit message is incorrect",
                    ex.getMessage().contains("System exit was initiated"));
        }
    }

    @Test
    public void testSitupInitiateExecute() {
        Situp testSitup = Situp.getInstance();
        boolean executionStatus = testSitup.execute(VALID_MULTIPLE_PIPELINE_CONFIG_FILE);
        assertThat("Failed to initiate execution", executionStatus);
        testSitup.shutdown();
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
