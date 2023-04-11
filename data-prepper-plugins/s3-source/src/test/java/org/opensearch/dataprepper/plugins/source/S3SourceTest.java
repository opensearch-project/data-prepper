/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class S3SourceTest {
    private final String PLUGIN_NAME = "s3";
    private final String TEST_PIPELINE_NAME = "test_pipeline";

    private S3Source s3Source;
    private PluginMetrics pluginMetrics;
    private S3SourceConfig s3SourceConfig;
    private PluginFactory pluginFactory;
    private AcknowledgementSetManager acknowledgementSetManager;


    @BeforeEach
    void setUp() {
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);
        s3SourceConfig = mock(S3SourceConfig.class);
        pluginFactory = mock(PluginFactory.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);

        when(s3SourceConfig.getCodec()).thenReturn(mock(PluginModel.class));

        s3Source = new S3Source(pluginMetrics, s3SourceConfig, pluginFactory, acknowledgementSetManager);
    }

    @Test
    void start_should_throw_IllegalStateException_when_buffer_is_null() {
        assertThrows(IllegalStateException.class, () -> s3Source.start(null));
    }
}
