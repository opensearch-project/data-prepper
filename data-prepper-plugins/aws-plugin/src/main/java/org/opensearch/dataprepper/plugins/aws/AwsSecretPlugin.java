package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;

@DataPrepperExtensionPlugin(modelType = AwsSecretPluginConfig.class, rootKeyJsonPath = "/aws/secrets",
        allowInPipelineConfigurations = true)
public class AwsSecretPlugin implements ExtensionPlugin {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final PluginConfigValueTranslator pluginConfigValueTranslator;

    @DataPrepperPluginConstructor
    public AwsSecretPlugin(final AwsSecretPluginConfig awsSecretPluginConfig) {
        if (awsSecretPluginConfig != null) {
            final SecretsSupplier secretsSupplier = new AwsSecretsSupplier(awsSecretPluginConfig, OBJECT_MAPPER);
            pluginConfigValueTranslator = new AwsSecretsPluginConfigValueTranslator(secretsSupplier);
        } else {
            pluginConfigValueTranslator = null;
        }
    }

    @Override
    public void apply(final ExtensionPoints extensionPoints) {
        extensionPoints.addExtensionProvider(new AwsSecretExtensionProvider(pluginConfigValueTranslator));
    }
}
