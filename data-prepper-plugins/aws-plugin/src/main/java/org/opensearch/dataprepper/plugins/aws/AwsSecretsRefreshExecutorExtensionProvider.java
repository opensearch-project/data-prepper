package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;

import java.util.Optional;

public class AwsSecretsRefreshExecutorExtensionProvider implements ExtensionProvider<PluginConfigPublisher> {

    private final AwsSecretsRefreshExecutorWrapper awsSecretsRefreshExecutorWrapper;

    AwsSecretsRefreshExecutorExtensionProvider(
            final AwsSecretsRefreshExecutorWrapper awsSecretsRefreshExecutorWrapper) {
        this.awsSecretsRefreshExecutorWrapper = awsSecretsRefreshExecutorWrapper;
    }

    @Override
    public Optional<PluginConfigPublisher> provideInstance(final Context context) {
        return Optional.empty();
    }

    @Override
    public Class<PluginConfigPublisher> supportedClass() {
        return PluginConfigPublisher.class;
    }
}
