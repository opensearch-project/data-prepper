/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.plugin.kafka.AuthConfig;
import org.opensearch.dataprepper.model.plugin.kafka.AwsConfig;
import org.opensearch.dataprepper.model.plugin.kafka.EncryptionConfig;
import org.opensearch.dataprepper.model.plugin.kafka.UsesKafkaClusterConfig;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;

import java.util.List;

@DataPrepperPlugin(name = "test_source_uses_kafka_cluster_config", pluginType = Source.class)
public class TestSourceUsingKafkaClusterConfig implements Source<Record<String>>, UsesKafkaClusterConfig {
    @Override
    public void setBootstrapServers(List<String> bootstrapServers) {

    }

    @Override
    public void setKafkaClusterAuthConfig(AuthConfig authConfig, AwsConfig awsConfig, EncryptionConfig encryptionConfig) {

    }

    @Override
    public void start(Buffer<Record<String>> buffer) {

    }

    @Override
    public void stop() {

    }
}
