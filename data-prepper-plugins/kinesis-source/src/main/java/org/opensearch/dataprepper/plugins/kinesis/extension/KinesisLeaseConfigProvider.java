package org.opensearch.dataprepper.plugins.kinesis.extension;

import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

class KinesisLeaseConfigProvider implements ExtensionProvider<KinesisLeaseConfigSupplier> {
    private final KinesisLeaseConfigSupplier kinesisLeaseConfigSupplier;

    public KinesisLeaseConfigProvider(final KinesisLeaseConfigSupplier kinesisLeaseConfigSupplier) {
        this.kinesisLeaseConfigSupplier = kinesisLeaseConfigSupplier;
    }

    @Override
    public Optional<KinesisLeaseConfigSupplier> provideInstance(Context context) {
        return Optional.of(this.kinesisLeaseConfigSupplier);
    }

    @Override
    public Class<KinesisLeaseConfigSupplier> supportedClass() {
        return KinesisLeaseConfigSupplier.class;
    }
}
