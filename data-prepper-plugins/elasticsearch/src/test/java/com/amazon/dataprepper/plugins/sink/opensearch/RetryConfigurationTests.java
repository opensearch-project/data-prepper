/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.sink.opensearch;

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
