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

import static com.google.common.base.Preconditions.checkArgument;

/**
 * S3 DLQ Provider for loading the S3 Writer.
 *
 * @since 2.2
 */
@DataPrepperPlugin(name = "s3",
    pluginType = DlqProvider.class,
    pluginConfigurationType = S3DlqWriterConfig.class)
public class S3DlqProvider implements DlqProvider {

    private static final String S3_DLQ_PLUGIN_NAME = "s3";

    private final S3DlqWriterConfig s3DlqWriterConfig;

    @DataPrepperPluginConstructor
    public S3DlqProvider(final S3DlqWriterConfig s3DlqWriterConfig) {
        Objects.requireNonNull(s3DlqWriterConfig);
        this.s3DlqWriterConfig = s3DlqWriterConfig;
    }

    @Override
    public Optional<DlqWriter> getDlqWriter(final String pluginMetricsScope) {
        checkArgument(pluginMetricsScope == null || !pluginMetricsScope.isEmpty(), "missing pluginMetricsScope for DLQ Writer");
        final PluginMetrics pluginMetrics = PluginMetrics.fromNames(S3_DLQ_PLUGIN_NAME, pluginMetricsScope);
        final ObjectMapper objectMapper = new ObjectMapper();
        return Optional.of(new S3DlqWriter(s3DlqWriterConfig, objectMapper, pluginMetrics));
    }
}
