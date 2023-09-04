/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.extension;

import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.EncryptionConfig;

import java.util.List;

/**
 * An interface available to plugins via the Kafka Cluster Config Plugin Extension which supplies
 * bootstrap_servers, authentication,encryption and aws configs.
 */
public interface KafkaClusterConfigSupplier {
    List<String> getBootStrapServers();

    AuthConfig getAuthConfig();

    AwsConfig getAwsConfig();

    EncryptionConfig getEncryptionConfig();
}
