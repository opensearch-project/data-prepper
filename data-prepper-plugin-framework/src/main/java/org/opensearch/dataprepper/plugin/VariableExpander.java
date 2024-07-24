/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator.DEFAULT_DEPRECATED_PREFIX;

@Named
public class VariableExpander {
    private static final Logger LOG = LoggerFactory.getLogger(VariableExpander.class);
    static final String REFERENCE_KEY = "referenceKey";
    static final String VALUE_REFERENCE_KEY = "valueReferenceKey";
    static final String REFERENCE_PATTERN_STRING = "^\\$\\{\\{(?<%s>.*)\\}\\}$";
    static final Pattern REFERENCE_PATTERN = Pattern.compile(
            String.format(REFERENCE_PATTERN_STRING, REFERENCE_KEY));
    static final String SECRETS_REFERENCE_PATTERN_STRING = "^\\$\\{\\{%s\\:(?<%s>.*)\\}\\}$";
    private final Map<Pattern, PluginConfigValueTranslator> patternPluginConfigValueTranslatorMap;
    private final Set<String> pluginConfigValueTranslatorPrefixes;
    private final ObjectMapper objectMapper;

    @Inject
    public VariableExpander(
            @Named("extensionPluginConfigObjectMapper")
            final ObjectMapper objectMapper,
            final Set<PluginConfigValueTranslator> pluginConfigValueTranslators) {
        this.objectMapper = objectMapper;
        patternPluginConfigValueTranslatorMap = new HashMap<>();
        pluginConfigValueTranslatorPrefixes = new HashSet<>();
        pluginConfigValueTranslators.forEach(pluginConfigValueTranslator -> {
            if (!DEFAULT_DEPRECATED_PREFIX.equals(
                    pluginConfigValueTranslator.getDeprecatedPrefix())) {
                patternPluginConfigValueTranslatorMap.put(
                        Pattern.compile(
                                String.format(SECRETS_REFERENCE_PATTERN_STRING,
                                        pluginConfigValueTranslator.getDeprecatedPrefix(), VALUE_REFERENCE_KEY)),
                        pluginConfigValueTranslator);
                pluginConfigValueTranslatorPrefixes.add(pluginConfigValueTranslator.getDeprecatedPrefix());
            }
            patternPluginConfigValueTranslatorMap.put(
                    Pattern.compile(
                            String.format(SECRETS_REFERENCE_PATTERN_STRING,
                                    pluginConfigValueTranslator.getPrefix(), VALUE_REFERENCE_KEY)),
                    pluginConfigValueTranslator);
            pluginConfigValueTranslatorPrefixes.add(pluginConfigValueTranslator.getPrefix());
        });
    }

    public <T> T translate(final JsonParser jsonParser, final Class<T> destinationType) throws IOException {
        if (JsonToken.VALUE_STRING.equals(jsonParser.currentToken())) {
            final String rawValue = jsonParser.getValueAsString();
            final Matcher referenceMatcher = REFERENCE_PATTERN.matcher(rawValue);
            if (referenceMatcher.matches()) {
                LOG.debug("Searching for available pluginConfigValueTranslator prefixes: {} " +
                        "to match prefix on the raw value: {}",
                        pluginConfigValueTranslatorPrefixes,
                        rawValue);
                return patternPluginConfigValueTranslatorMap.entrySet().stream()
                        .map(entry -> Map.entry(entry.getKey().matcher(rawValue), entry.getValue()))
                        .filter(entry -> entry.getKey().matches())
                        .map(entry -> {
                            final String valueReferenceKey = entry.getKey().group(VALUE_REFERENCE_KEY);
                            return objectMapper.convertValue(
                                    entry.getValue().translate(valueReferenceKey), destinationType);
                        })
                        .findFirst()
                        .orElseGet(() -> {
                            LOG.warn("No available PluginConfigValueTranslator prefixes: {} " +
                                    "match prefix on the raw value: {}, it will be treated as literal value",
                                    pluginConfigValueTranslatorPrefixes,
                                    rawValue);
                            return objectMapper.convertValue(rawValue, destinationType);
                        });
            }
            return objectMapper.convertValue(rawValue, destinationType);
        }
        return objectMapper.readValue(jsonParser, destinationType);
    }
}
