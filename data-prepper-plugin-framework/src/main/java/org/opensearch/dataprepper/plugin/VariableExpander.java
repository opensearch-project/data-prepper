/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Named
public class VariableExpander {
    static final String VALUE_REFERENCE_KEY = "valueReferenceKey";
    static final String SECRETS_REFERENCE_PATTERN_STRING = "^\\$\\{\\{%s\\:(?<%s>.*)\\}\\}$";
    private final Map<Pattern, PluginConfigValueTranslator> patternPluginConfigValueTranslatorMap;
    private final ObjectMapper objectMapper;

    @Inject
    public VariableExpander(
            @Named("extensionPluginConfigObjectMapper") final ObjectMapper objectMapper,
            final Set<PluginConfigValueTranslator> pluginConfigValueTranslators) {
        this.objectMapper = objectMapper;
        patternPluginConfigValueTranslatorMap = pluginConfigValueTranslators.stream().collect(Collectors.toMap(
                pluginConfigValueTranslator -> Pattern.compile(
                        String.format(SECRETS_REFERENCE_PATTERN_STRING,
                                pluginConfigValueTranslator.getPrefix(), VALUE_REFERENCE_KEY)),
                Function.identity()));
    }

    public <T> T translate(final JsonParser jsonParser, final Class<T> destinationType) throws IOException {
        if (JsonToken.VALUE_STRING.equals(jsonParser.currentToken())) {
            final String rawValue = jsonParser.getValueAsString();
            return patternPluginConfigValueTranslatorMap.entrySet().stream()
                    .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey().matcher(rawValue), entry.getValue()))
                    .filter(entry -> entry.getKey().matches())
                    .map(entry -> {
                        final String valueReferenceKey = entry.getKey().group(VALUE_REFERENCE_KEY);
                        if (destinationType.equals(PluginConfigVariable.class)) {
                            return (T) entry.getValue().translateToPluginConfigVariable(valueReferenceKey);
                        } else {
                            return objectMapper.convertValue(
                                    entry.getValue().translate(valueReferenceKey), destinationType);
                        }

                    })
                    .findFirst()
                    .orElseGet(() -> {
                        // This change is to support any call
                        // to validate the secret with a placeholder secret expression like "AWS_SECRET_EXPRESSION"
                        // which is not of a secrets expression that we would check (filter check above fails) but
                        // still expects an instance of PluginConfigVariable object returned
                        if (destinationType.equals(PluginConfigVariable.class)) {
                            return destinationType.cast(new ImmutablePluginConfigVariable(rawValue));
                        }
                        return objectMapper.convertValue(rawValue, destinationType);
                    });
        }
        return objectMapper.readValue(jsonParser, destinationType);
    }

    private static class ImmutablePluginConfigVariable implements PluginConfigVariable {
        private final Object rawValue;

        private ImmutablePluginConfigVariable(final Object rawValue) {
            this.rawValue = rawValue;
        }

        @Override
        public Object getValue() {
            return rawValue;
        }

        @Override
        public void setValue(Object updatedValue) {
            throw new UnsupportedOperationException("ImmutablePluginConfigVariable doesn't support this operation");
        }

        @Override
        public void refresh() {
            // No-op as this is immutable
            throw new UnsupportedOperationException("ImmutablePluginConfigVariable doesn't support this operation");
        }

        @Override
        public boolean isUpdatable() {
            return false;
        }
    }

}
