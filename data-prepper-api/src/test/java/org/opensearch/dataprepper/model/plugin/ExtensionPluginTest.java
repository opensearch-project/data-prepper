package org.opensearch.dataprepper.model.plugin;

import org.junit.jupiter.api.Test;

public class ExtensionPluginTest {

    @Test
    void testShutdown() {
        final ExtensionPlugin extensionPlugin = new ExtensionPluginTestImpl();
        extensionPlugin.shutdown();
    }

    static class ExtensionPluginTestImpl implements ExtensionPlugin {

        @Override
        public void apply(ExtensionPoints extensionPoints) {

        }
    }
}
