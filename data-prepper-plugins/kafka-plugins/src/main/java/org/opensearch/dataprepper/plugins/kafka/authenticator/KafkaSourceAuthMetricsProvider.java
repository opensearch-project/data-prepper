/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kafka.authenticator;

/**
 * Process-global handoff for the azure_federated auth metrics object, mirroring
 * {@link AwsCredentialsSupplierProvider}. {@link AzureFederatedCallbackHandler} is instantiated by
 * kafka-clients via reflection (no-arg constructor), so a {@code PluginMetrics}-backed object cannot
 * be passed down the constructor chain; the source publishes it here before Kafka client construction
 * and the handler reads it in {@code configure()}.
 *
 * <p>Like {@link AwsCredentialsSupplierProvider}, this is a plain last-writer-wins singleton. If two
 * federated Kafka sources ever ran in the same JVM, the second {@code set()} would overwrite the
 * first and its refreshes would be attributed to the second source's metric prefix. This does not
 * occur in the target deployment (one Kafka source per container), and it is the same limitation the
 * sibling {@link AwsCredentialsSupplierProvider} already accepts.
 */
public class KafkaSourceAuthMetricsProvider {
    private static final KafkaSourceAuthMetricsProvider singleton = new KafkaSourceAuthMetricsProvider();

    public static KafkaSourceAuthMetricsProvider getInstance() {
        return singleton;
    }

    private volatile KafkaSourceAuthMetrics authMetrics;

    protected KafkaSourceAuthMetricsProvider() {}

    public KafkaSourceAuthMetrics getAuthMetrics() {
        return authMetrics;
    }

    public void set(final KafkaSourceAuthMetrics authMetrics) {
        this.authMetrics = authMetrics;
    }
}
