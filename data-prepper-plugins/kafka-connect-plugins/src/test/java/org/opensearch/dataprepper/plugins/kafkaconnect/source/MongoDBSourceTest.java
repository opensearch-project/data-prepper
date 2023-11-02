/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.source;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.MongoDBConfig;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MongoDBSourceTest {
    @Mock
    private MongoDBConfig mongoDBConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;


    @Test
    void testConstructorValidations() {
        assertThrows(IllegalArgumentException.class, () -> new MongoDBSource(
                mongoDBConfig,
                pluginMetrics,
                pipelineDescription,
                acknowledgementSetManager,
                awsCredentialsSupplier,
                null,
                null));
    }
}
