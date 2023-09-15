package org.opensearch.dataprepper.model.plugin;

public interface PluginConfigValueTranslator {
    Object translate(final String value);

    String getPrefix();
}
