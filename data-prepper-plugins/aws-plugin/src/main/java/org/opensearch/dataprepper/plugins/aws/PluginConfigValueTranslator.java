package org.opensearch.dataprepper.plugins.aws;

public interface PluginConfigValueTranslator {
    String translate(final String value);
}
