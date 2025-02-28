/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.opensearch.dataprepper.model.plugin.InvalidPipelineConfigurationException;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Named
public class PipelineConfigurationErrorHandler {
    static final String UNRECOGNIZED_PROPERTY_EXCEPTION_FORMAT = "Parameter \"%s\" does not exist. Available options include %s.";
    static final String JSON_MAPPING_EXCEPTION_FORMAT = "Parameter \"%s\" is invalid: %s";

    private final ClosestFieldRecommender closestFieldRecommender;

    @Inject
    public PipelineConfigurationErrorHandler(final ClosestFieldRecommender closestFieldRecommender) {
        this.closestFieldRecommender = closestFieldRecommender;
    }

    public RuntimeException handleException(final Exception e) {
        if (e instanceof UnrecognizedPropertyException) {
            return handleUnrecognizedPropertyException((UnrecognizedPropertyException) e);
        } else if (e instanceof JsonMappingException) {
            return handleJsonMappingException((JsonMappingException) e);
        }

        return new InvalidPipelineConfigurationException(e.getMessage());
    }
    private RuntimeException handleJsonMappingException(final JsonMappingException e) {
        final String parameterPath = getParameterPath(e.getPath());

        final String errorMessage = String.format(JSON_MAPPING_EXCEPTION_FORMAT, parameterPath, e.getOriginalMessage());

        return new InvalidPipelineConfigurationException(errorMessage);
    }

    private RuntimeException handleUnrecognizedPropertyException(final UnrecognizedPropertyException e) {
        String errorMessage = String.format(UNRECOGNIZED_PROPERTY_EXCEPTION_FORMAT,
                getParameterPath(e.getPath()), e.getKnownPropertyIds());

        final Optional<String> closestRecommendation = closestFieldRecommender.getClosestField(
                e.getPropertyName(), e.getKnownPropertyIds());

        if (closestRecommendation.isPresent()) {
            errorMessage += " Did you mean \"" + closestRecommendation.get() + "\"?";
        }

        return new InvalidPipelineConfigurationException(errorMessage);
    }

    private String getParameterPath(final List<JsonMappingException.Reference> path) {
        return path.stream()
                .map(JsonMappingException.Reference::getFieldName)
                .collect(Collectors.joining("."));
    }
}
