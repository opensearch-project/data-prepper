package com.amazon.situp.plugins.sink.elasticsearch;

import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RetryConfigurationTests {
    @Test
    public void testDefaultConfigurationIsNotNull() {
        final RetryConfiguration retryConfiguration = new RetryConfiguration.Builder().build();
        assertNull(retryConfiguration.getDlqFile());
        final Integer expStatus = RestStatus.TOO_MANY_REQUESTS.getStatus();
        assertEquals(new HashSet<>(Collections.singletonList(expStatus)), retryConfiguration.getRetryStatus());
    }

    @Test
    public void testConfigurationWithCustomDlqFilePath() {
        final String fakeDlqFilePath = "foo.txt";
        final RetryConfiguration retryConfiguration = new RetryConfiguration.Builder()
                .withDlqFile(fakeDlqFilePath)
                .build();
        assertEquals(fakeDlqFilePath, retryConfiguration.getDlqFile());
    }

    @Test
    public void testConfigurationWithCustomRestStatus() {
        final List<Integer> testStatus = Arrays.asList(400, 404, 429);
        final RetryConfiguration retryConfiguration = new RetryConfiguration.Builder()
                .withRetryStatus(testStatus)
                .build();
        assertEquals(new HashSet<>(testStatus), retryConfiguration.getRetryStatus());
    }
}
