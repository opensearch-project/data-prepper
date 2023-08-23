package org.opensearch.dataprepper.plugins.aws;

public class NoopPluginConfigValueTranslator implements PluginConfigValueTranslator {
    @Override
    public String translate(String value) {
        return value;
    }
}
