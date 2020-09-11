package com.amazon.situp.plugins.sink.elasticsearch;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RetryConfigurationTests {
    @Test
    public void testDefault() {
        RetryConfiguration retryConfiguration = new RetryConfiguration.Builder().build();
        assertNull(retryConfiguration.getDlqFile());
    }

    @Test
    public void testWithFile() {
        String fakeDlqFilePath = "foo.txt";
        RetryConfiguration retryConfiguration = new RetryConfiguration.Builder()
                .withDlqFile(fakeDlqFilePath)
                .build();
        assertEquals(fakeDlqFilePath, retryConfiguration.getDlqFile());
    }
}
