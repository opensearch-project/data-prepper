/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.extension;

public interface KafkaConnectConfigSupplier {
    KafkaConnectConfig getConfig();
}
