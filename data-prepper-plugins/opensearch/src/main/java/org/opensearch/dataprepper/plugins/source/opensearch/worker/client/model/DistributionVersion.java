/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

public enum DistributionVersion {
    ES7("es7"),
    OPENSEARCH("opensearch");

    private final String distributionVersion;
    DistributionVersion(final String distributionVersion) {
        this.distributionVersion = distributionVersion;
    }
}
