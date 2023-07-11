/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.plugin.kafka;

public interface KafkaClusterAuthConfig {
    AwsConfig getAwsConfig();

    AuthConfig getAuthConfig();

    EncryptionConfig getEncryptionConfig();

    String getBootStrapServers();
}
