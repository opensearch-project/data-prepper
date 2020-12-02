package com.amazon.situp.plugins.sink.elasticsearch;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RetryConfigurationTests {
    @Test
    public void testDefaultConfigurationIsNotNull() {
        final RetryConfiguration retryConfiguration = new RetryConfiguration.Builder().build();
        assertNull(retryConfiguration.getDlqFile());
    }

    @Test
    public void testConfigurationWithCustomDlqFilePath() {
        final String fakeDlqFilePath = "foo.txt";
        final RetryConfiguration retryConfiguration = new RetryConfiguration.Builder()
                .withDlqFile(fakeDlqFilePath)
                .build();
        assertEquals(fakeDlqFilePath, retryConfiguration.getDlqFile());
    }
}
