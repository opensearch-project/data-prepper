/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.plugin.kafka;

import java.util.List;

/**
 * An interface that a plugin who uses Kafka Cluster will implement when kafka_cluster_config is configured
 */
public interface UsesKafkaClusterConfig {

    void setBootstrapServers(final List<String> bootstrapServers);

    void setKafkaClusterAuthConfig(final AuthConfig authConfig,
                                   final AwsConfig awsConfig,
                                   final EncryptionConfig encryptionConfig);
}
