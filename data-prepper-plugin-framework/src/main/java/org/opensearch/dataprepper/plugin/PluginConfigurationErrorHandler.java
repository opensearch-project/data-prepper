package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Named
public class PluginConfigurationErrorHandler {

    static final String UNRECOGNIZED_PROPERTY_EXCEPTION_FORMAT = "Parameter \"%s\" for plugin \"%s\" does not exist. Available options include %s.";
    static final String JSON_MAPPING_EXCEPTION_FORMAT = "Parameter \"%s\" for plugin \"%s\" is invalid: %s";
    static final String GENERIC_PLUGIN_EXCEPTION_FORMAT = "Plugin \"%s\" is invalid: %s";

    static final Integer MIN_DISTANCE_TO_RECOMMEND_PROPERTY = 3;

    private final LevenshteinDistance levenshteinDistance;

    @Inject
    public PluginConfigurationErrorHandler(final LevenshteinDistance levenshteinDistance) {
        this.levenshteinDistance = levenshteinDistance;
    }

    public RuntimeException handleException(final PluginSetting pluginSetting, final Exception e) {
        if (e.getCause() instanceof UnrecognizedPropertyException) {
            return handleUnrecognizedPropertyException((UnrecognizedPropertyException) e.getCause(), pluginSetting);
        } else if (e.getCause() instanceof JsonMappingException) {
            return handleJsonMappingException((JsonMappingException) e.getCause(), pluginSetting);
        }

        return new InvalidPluginConfigurationException(
                String.format(GENERIC_PLUGIN_EXCEPTION_FORMAT, pluginSetting.getName(), e.getMessage()));
    }

    private RuntimeException handleJsonMappingException(final JsonMappingException e, final PluginSetting pluginSetting) {
        final String parameterPath = getParameterPath(e.getPath());

        final String errorMessage = String.format(JSON_MAPPING_EXCEPTION_FORMAT,
                parameterPath, pluginSetting.getName(), e.getOriginalMessage());

        return new InvalidPluginConfigurationException(errorMessage);
    }

    private RuntimeException handleUnrecognizedPropertyException(final UnrecognizedPropertyException e, final PluginSetting pluginSetting) {
        String errorMessage = String.format(UNRECOGNIZED_PROPERTY_EXCEPTION_FORMAT,
                getParameterPath(e.getPath()),
                pluginSetting.getName(),
                e.getKnownPropertyIds());

        final Optional<String> closestRecommendation = getClosestField(e);

        if (closestRecommendation.isPresent()) {
            errorMessage += " Did you mean \"" + closestRecommendation.get() + "\"?";
        }

        return new InvalidPluginConfigurationException(errorMessage);
    }

    private Optional<String> getClosestField(final UnrecognizedPropertyException e) {
        String closestMatch = null;
        int smallestDistance = Integer.MAX_VALUE;

        for (final String field : e.getKnownPropertyIds().stream().map(Object::toString).collect(Collectors.toList())) {
            int distance = levenshteinDistance.apply(e.getPropertyName(), field);

            if (distance < smallestDistance) {
                smallestDistance = distance;
                closestMatch = field;
            }
        }

        if (smallestDistance <= MIN_DISTANCE_TO_RECOMMEND_PROPERTY) {
            return Optional.ofNullable(closestMatch);
        }

        return Optional.empty();
    }

    private String getParameterPath(final List<JsonMappingException.Reference> path) {
        return path.stream()
                .map(JsonMappingException.Reference::getFieldName)
                .collect(Collectors.joining("."));
    }
}
