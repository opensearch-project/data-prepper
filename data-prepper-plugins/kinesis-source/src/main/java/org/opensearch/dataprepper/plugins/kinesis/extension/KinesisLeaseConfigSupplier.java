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
