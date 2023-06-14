package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.aws.api.SecretsSupplier;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

public class AwsSecretExtensionProvider implements ExtensionProvider<SecretsSupplier> {
    private final SecretsSupplier secretsSupplier;

    AwsSecretExtensionProvider(final SecretsSupplier secretsSupplier) {
        this.secretsSupplier = secretsSupplier;
    }
    @Override
    public Optional<SecretsSupplier> provideInstance(Context context) {
        return Optional.of(secretsSupplier);
    }

    @Override
    public Class<SecretsSupplier> supportedClass() {
        return SecretsSupplier.class;
    }
}
