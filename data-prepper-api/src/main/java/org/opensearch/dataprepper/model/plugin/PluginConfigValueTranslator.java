package org.opensearch.dataprepper.model.plugin;

public interface PluginConfigValueTranslator {
    String translate(final String value);

    String getKey();
}
