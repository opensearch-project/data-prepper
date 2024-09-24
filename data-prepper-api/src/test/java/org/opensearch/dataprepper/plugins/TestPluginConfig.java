package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.processor.Processor;

@DataPrepperPlugin(name = "test_plugin", pluginType = Processor.class)
public class TestPluginConfig {
}
