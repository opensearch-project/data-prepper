/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.plugins.source.configuration.SqsOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

        final SqsOptions sqsOptions = mock(SqsOptions.class);
        when(s3SourceConfig.getSqsOptions()).thenReturn(sqsOptions);
        when(sqsOptions.getSqsUrl()).thenReturn("https://sqs/123/abc");

        s3Source = new S3Source(pluginMetrics, s3SourceConfig);
    }

    @Test
    void start_should_throw_IllegalStateException_when_buffer_is_null() {
        assertThrows(IllegalStateException.class, () -> s3Source.start(null));
    }
}