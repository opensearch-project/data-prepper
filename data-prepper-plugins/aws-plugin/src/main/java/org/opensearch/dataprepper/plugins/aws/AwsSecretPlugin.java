package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;

@DataPrepperExtensionPlugin(modelType = AwsSecretPluginConfig.class, rootKey = "aws_secrets")
public class AwsSecretPlugin implements ExtensionPlugin {
    private final AwsSecretsSupplier awsSecretsSupplier;

    @DataPrepperPluginConstructor
    public AwsSecretPlugin(final AwsSecretPluginConfig awsSecretPluginConfig) {
        awsSecretsSupplier = new AwsSecretsSupplier(awsSecretPluginConfig);
    }

    @Override
    public void apply(final ExtensionPoints extensionPoints) {
        extensionPoints.addExtensionProvider(new AwsSecretExtensionProvider(awsSecretsSupplier));
    }
}
