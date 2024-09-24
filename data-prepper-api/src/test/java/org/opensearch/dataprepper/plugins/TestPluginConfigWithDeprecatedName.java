package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.processor.Processor;

@DataPrepperPlugin(name = "test_plugin_config_with_deprecated_name",
        deprecatedName = "test_plugin_deprecated", pluginType = Processor.class)
public class TestPluginConfigWithDeprecatedName {
}
