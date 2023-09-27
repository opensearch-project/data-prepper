package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;

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
            @Named("extensionPluginConfigObjectMapper")
            final ObjectMapper objectMapper,
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
                        return objectMapper.convertValue(
                                entry.getValue().translate(valueReferenceKey), destinationType);
                    })
                    .findFirst()
                    .orElseGet(() -> objectMapper.convertValue(rawValue, destinationType));
        }
        return objectMapper.readValue(jsonParser, destinationType);
    }
}
