package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;

public class NoopPluginConfigValueTranslator implements PluginConfigValueTranslator {
    @Override
    public String translate(String value) {
        return value;
    }

    @Override
    public String getPrefix() {
        return null;
    }
}
