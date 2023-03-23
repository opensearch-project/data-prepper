/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class RetryConfigurationTests {
    @Test
    public void testDefaultConfigurationIsNotNull() {
        final RetryConfiguration retryConfiguration = new RetryConfiguration.Builder().build();
        assertNull(retryConfiguration.getDlqFile());
        assertEquals(retryConfiguration.getMaxRetries(), Integer.MAX_VALUE);
    }

    @Test
    public void testReadRetryConfigInvalidMaxRetries() {
        final RetryConfiguration retryConfiguration = RetryConfiguration.readRetryConfig(generatePluginSetting(null, -1));
        assertThrows(IllegalArgumentException.class, () -> retryConfiguration.getMaxRetries());
    }

    @Test
    public void testReadRetryConfigNoDLQFilePath() {
        final RetryConfiguration retryConfiguration = RetryConfiguration.readRetryConfig(generatePluginSetting(null, null));
        assertNull(retryConfiguration.getDlqFile());
        assertEquals(retryConfiguration.getMaxRetries(), Integer.MAX_VALUE);
    }

    @Test
    public void testReadRetryConfigWithDLQFilePath() {
        final String fakeDlqFilePath = "foo.txt";
        final int maxRetries = 10;
        final RetryConfiguration retryConfiguration = RetryConfiguration.readRetryConfig(generatePluginSetting(fakeDlqFilePath, maxRetries));
        assertEquals(fakeDlqFilePath, retryConfiguration.getDlqFile());
        assertEquals(maxRetries, retryConfiguration.getMaxRetries());
    }

    private PluginSetting generatePluginSetting(final String dlqFilePath, Integer maxRetries) {
        final Map<String, Object> metadata = new HashMap<>();
        if (dlqFilePath != null) {
            metadata.put(RetryConfiguration.DLQ_FILE, dlqFilePath);
        }
        if (maxRetries != null) {
            metadata.put(RetryConfiguration.MAX_RETRIES, maxRetries);
        }

        return new PluginSetting("opensearch", metadata);
    }
}
