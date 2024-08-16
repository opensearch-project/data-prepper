package org.opensearch.dataprepper.plugins.kinesis.extension;

import java.util.Optional;

public class DefaultKinesisLeaseConfigSupplier implements KinesisLeaseConfigSupplier {

    private KinesisLeaseConfig kinesisLeaseConfig;

    public DefaultKinesisLeaseConfigSupplier(final KinesisLeaseConfig kinesisLeaseConfig) {
        this.kinesisLeaseConfig = kinesisLeaseConfig;
    }

    @Override
    public Optional<KinesisLeaseConfig> getKinesisExtensionLeaseConfig() {
        return kinesisLeaseConfig != null ? Optional.of(kinesisLeaseConfig) : Optional.empty();
    }
}
