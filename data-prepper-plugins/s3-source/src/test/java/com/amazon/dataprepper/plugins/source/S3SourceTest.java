/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.metrics.PluginMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class S3SourceTest {
    private final String PLUGIN_NAME = "s3";
    private final String TEST_PIPELINE_NAME = "test_pipeline";

    private S3Source s3Source;
    private PluginMetrics pluginMetrics;
    private S3SourceConfig s3SourceConfig;


    @BeforeEach
    void setUp() {
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);
        s3SourceConfig = mock(S3SourceConfig.class);

        s3Source = new S3Source(pluginMetrics, s3SourceConfig);
    }

    @Test
    void start_should_throw_IllegalStateException_when_buffer_is_null() {
        assertThrows(IllegalStateException.class, () -> s3Source.start(null));
    }
}