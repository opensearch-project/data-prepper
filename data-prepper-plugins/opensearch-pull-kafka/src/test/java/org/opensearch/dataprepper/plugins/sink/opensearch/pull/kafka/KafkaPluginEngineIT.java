/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.pull.kafka;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.opensearch.PullEngine;
import org.opensearch.dataprepper.test.plugins.DataPrepperPluginTest;
import org.opensearch.dataprepper.test.plugins.PluginConfigurationFile;
import org.opensearch.dataprepper.test.plugins.junit.BaseDataPrepperPluginStandardTestSuite;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@DataPrepperPluginTest(pluginName = "kafka", pluginType = PullEngine.class)
public class KafkaPluginEngineIT extends BaseDataPrepperPluginStandardTestSuite {
    @Test
    void constructs_with_valid_configuration(
            @PluginConfigurationFile("valid_kafka_config.yaml") final PullEngine pullEngine) {
        assertThat(pullEngine, notNullValue());
        assertThat(pullEngine, instanceOf(KafkaPullEngine.class));
    }
}
