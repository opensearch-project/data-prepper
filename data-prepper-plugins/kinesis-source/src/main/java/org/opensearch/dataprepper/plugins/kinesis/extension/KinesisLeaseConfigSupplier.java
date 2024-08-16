package org.opensearch.dataprepper.plugins.kinesis.extension;

import java.util.Optional;

public interface KinesisLeaseConfigSupplier {

    default Optional<KinesisLeaseConfig> getKinesisExtensionLeaseConfig() {
        return Optional.empty();
    }
}
