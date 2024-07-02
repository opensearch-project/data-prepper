package org.opensearch.dataprepper.model.plugin;

public interface PluginConfigValueTranslator {
    String DEFAULT_DEPRECATED_PREFIX = "";

    Object translate(final String value);

    default String getDeprecatedPrefix() {
        return DEFAULT_DEPRECATED_PREFIX;
    }

    String getPrefix();
}
