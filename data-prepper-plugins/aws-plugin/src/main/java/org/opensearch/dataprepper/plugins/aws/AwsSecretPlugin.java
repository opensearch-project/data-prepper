package org.opensearch.dataprepper.plugins.aws;

//import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;

//@DataPrepperExtensionPlugin(modelType = AwsSecretPluginConfig.class, rootKey = "aws_secrets")
public class AwsSecretPlugin implements ExtensionPlugin {
    @DataPrepperPluginConstructor
    public AwsSecretPlugin(final AwsSecretPluginConfig awsSecretPluginConfig) {
    }

    @Override
    public void apply(final ExtensionPoints extensionPoints) {
//        extensionPoints.addExtensionProvider(...);
    }
}
