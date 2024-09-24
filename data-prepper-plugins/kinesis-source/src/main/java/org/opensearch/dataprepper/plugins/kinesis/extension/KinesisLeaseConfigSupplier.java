/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.extension;

import java.util.Optional;

public class KinesisLeaseConfigSupplier {

    private KinesisLeaseConfig kinesisLeaseConfig;

    public KinesisLeaseConfigSupplier(final KinesisLeaseConfig kinesisLeaseConfig) {
        this.kinesisLeaseConfig = kinesisLeaseConfig;
    }

    public Optional<KinesisLeaseConfig> getKinesisExtensionLeaseConfig() {
        return Optional.ofNullable(kinesisLeaseConfig);
    }
}
