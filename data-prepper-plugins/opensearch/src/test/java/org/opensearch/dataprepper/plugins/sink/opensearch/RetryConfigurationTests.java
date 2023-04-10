/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class RetryConfigurationTests {
    @Test
    public void testDefaultConfigurationIsNotNull() {
        final RetryConfiguration retryConfiguration = new RetryConfiguration.Builder().build();
        assertNull(retryConfiguration.getDlqFile());
        assertEquals(retryConfiguration.getMaxRetries(), Integer.MAX_VALUE);
    }

    @Test
    public void testReadRetryConfigInvalidMaxRetries() {
        final RetryConfiguration retryConfiguration = RetryConfiguration.readRetryConfig(generatePluginSetting(null, -1, null));
        assertThrows(IllegalArgumentException.class, () -> retryConfiguration.getMaxRetries());
    }

    @Test
    public void testReadRetryConfigNoDLQFilePath() {
        final RetryConfiguration retryConfiguration = RetryConfiguration.readRetryConfig(generatePluginSetting(null, null, null));
        assertNull(retryConfiguration.getDlqFile());
        assertEquals(retryConfiguration.getMaxRetries(), Integer.MAX_VALUE);
        assertFalse(retryConfiguration.getDlq().isPresent());
    }

    @Test
    public void testReadRetryConfigWithDLQFilePath() {
        final String fakeDlqFilePath = "foo.txt";
        final int maxRetries = 10;
        final RetryConfiguration retryConfiguration = RetryConfiguration.readRetryConfig(generatePluginSetting(fakeDlqFilePath, maxRetries, null));
        assertEquals(fakeDlqFilePath, retryConfiguration.getDlqFile());
        assertEquals(maxRetries, retryConfiguration.getMaxRetries());
    }

    @Test
    public void testReadRetryConfigWithDLQPlugin() {
        final Map<String, Object> fakePlugin = new LinkedHashMap<>();
        final Map<String, Object> lowLevelPluginSettings = new HashMap<>();
        lowLevelPluginSettings.put("field1", "value1");
        lowLevelPluginSettings.put("field2", "value2");
        fakePlugin.put("another_dlq", lowLevelPluginSettings);
        final int maxRetries = 10;
        final RetryConfiguration retryConfiguration = RetryConfiguration.readRetryConfig(generatePluginSetting(null, maxRetries, fakePlugin));
        assertEquals("another_dlq", retryConfiguration.getDlq().get().getPluginName());
        assertEquals(maxRetries, retryConfiguration.getMaxRetries());
    }

    @Test
    public void testReadRetryConfigWithDLQPluginAndDLQFilePath() {
        final String fakeDlqFilePath = "foo.txt";
        final Map<String, Object> fakePlugin = new LinkedHashMap<>();
        fakePlugin.put("another_dlq", "value1");
        final int maxRetries = 10;
        assertThrows(RuntimeException.class, () -> RetryConfiguration.readRetryConfig(generatePluginSetting(fakeDlqFilePath, maxRetries, fakePlugin)));
    }

    private PluginSetting generatePluginSetting(final String dlqFilePath, final Integer maxRetries, final Map<String, Object> pluginSettings) {
        final Map<String, Object> metadata = new HashMap<>();
        if (dlqFilePath != null) {
            metadata.put(RetryConfiguration.DLQ_FILE, dlqFilePath);
        }
        if (maxRetries != null) {
            metadata.put(RetryConfiguration.MAX_RETRIES, maxRetries);
        }
        if (pluginSettings != null) {
            metadata.put(RetryConfiguration.DLQ, pluginSettings);
        }

        return new PluginSetting("opensearch", metadata);
    }
}
