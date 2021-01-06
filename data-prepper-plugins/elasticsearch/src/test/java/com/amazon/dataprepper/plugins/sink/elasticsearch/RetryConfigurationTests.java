package com.amazon.dataprepper.plugins.sink.elasticsearch;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RetryConfigurationTests {
    @Test
    public void testDefaultConfigurationIsNotNull() {
        final RetryConfiguration retryConfiguration = new RetryConfiguration.Builder().build();
        assertNull(retryConfiguration.getDlqFile());
    }

    @Test
    public void testReadRetryConfigNoDLQFilePath() {
        final RetryConfiguration retryConfiguration = RetryConfiguration.readRetryConfig(generatePluginSetting(null));
        assertNull(retryConfiguration.getDlqFile());
    }

    @Test
    public void testReadRetryConfigWithDLQFilePath() {
        final String fakeDlqFilePath = "foo.txt";
        final RetryConfiguration retryConfiguration = RetryConfiguration.readRetryConfig(generatePluginSetting(fakeDlqFilePath));
        assertEquals(fakeDlqFilePath, retryConfiguration.getDlqFile());
    }

    private PluginSetting generatePluginSetting(final String dlqFilePath) {
        final Map<String, Object> metadata = new HashMap<>();
        if (dlqFilePath != null) {
            metadata.put(RetryConfiguration.DLQ_FILE, dlqFilePath);
        }

        return new PluginSetting("elasticsearch", metadata);
    }
}
