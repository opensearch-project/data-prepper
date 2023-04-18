/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.dlq.s3;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;

import java.util.Objects;
import java.util.Optional;

/**
 * S3 DLQ Provider for loading the S3 Writer.
 *
 * @since 2.2
 */
@DataPrepperPlugin(name = "s3",
    pluginType = DlqProvider.class,
    pluginConfigurationType = S3DlqWriterConfig.class)
public class S3DlqProvider implements DlqProvider {

    private final S3DlqWriterConfig s3DlqWriterConfig;
    private final PluginMetrics pluginMetrics;

    @DataPrepperPluginConstructor
    public S3DlqProvider(final S3DlqWriterConfig s3DlqWriterConfig, final PluginMetrics pluginMetrics) {
        Objects.requireNonNull(s3DlqWriterConfig);
        this.s3DlqWriterConfig = s3DlqWriterConfig;
        this.pluginMetrics = pluginMetrics;
    }

    @Override
    public Optional<DlqWriter> getDlqWriter() {
        final ObjectMapper objectMapper = new ObjectMapper();
        return Optional.of(new S3DlqWriter(s3DlqWriterConfig, objectMapper, pluginMetrics));
    }
}
