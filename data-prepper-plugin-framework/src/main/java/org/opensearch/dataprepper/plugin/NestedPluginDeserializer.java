/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

class NestedPluginDeserializer extends JsonDeserializer<Object> {
    private static final Logger LOG = LoggerFactory.getLogger(NestedPluginDeserializer.class);
    static final String PLUGIN_FACTORY_ATTRIBUTE_KEY = "pluginFactory";

    private final Class<?> pluginType;

    NestedPluginDeserializer(final Class<?> pluginType) {
        this.pluginType = pluginType;
    }

    @Override
    public Object deserialize(final JsonParser parser, final DeserializationContext ctxt) throws IOException {
        final PluginFactory pluginFactory = (PluginFactory) ctxt.getAttribute(PLUGIN_FACTORY_ATTRIBUTE_KEY);
        if (pluginFactory == null) {
            throw ctxt.instantiationException(pluginType,
                    "PluginFactory is not available in the deserialization context");
        }

        if (parser.currentToken() != JsonToken.START_OBJECT) {
            throw ctxt.wrongTokenException(parser, Map.class, JsonToken.START_OBJECT,
                    "Nested plugin configuration must be a map with the plugin name as key");
        }

        parser.nextToken();
        final String pluginName = parser.currentName();
        parser.nextToken();

        final Map<String, Object> pluginSettings;
        if (parser.currentToken() == JsonToken.START_OBJECT) {
            pluginSettings = parser.readValueAs(Map.class);
        } else if (parser.currentToken() == JsonToken.VALUE_NULL) {
            pluginSettings = Collections.emptyMap();
        } else {
            pluginSettings = Collections.emptyMap();
        }

        parser.nextToken();
        if (parser.currentToken() != JsonToken.END_OBJECT) {
            LOG.warn("Plugin configuration for '{}' should have exactly one key (the plugin name), " +
                    "but additional keys were found. Only the first plugin '{}' will be used.",
                    pluginType.getSimpleName(), pluginName);
            while (parser.currentToken() != JsonToken.END_OBJECT) {
                parser.nextToken();
                parser.skipChildren();
                parser.nextToken();
            }
        }

        final PluginSetting pluginSetting = new PluginSetting(pluginName, pluginSettings);
        return pluginFactory.loadPlugin(pluginType, pluginSetting);
    }

    @Override
    public Object getNullValue(final DeserializationContext ctxt) {
        return null;
    }
}
