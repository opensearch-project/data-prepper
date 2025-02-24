/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.InvalidPipelineConfigurationException;

import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationErrorHandler.JSON_MAPPING_EXCEPTION_FORMAT;
import static org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationErrorHandler.MIN_DISTANCE_TO_RECOMMEND_PROPERTY;
import static org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationErrorHandler.UNRECOGNIZED_PROPERTY_EXCEPTION_FORMAT;

@ExtendWith(MockitoExtension.class)
class PipelineConfigurationErrorHandlerTest {
    @Mock
    private LevenshteinDistance levenshteinDistance;

    private PipelineConfigurationErrorHandler createObjectUnderTest() {
        return new PipelineConfigurationErrorHandler(levenshteinDistance);
    }

    @Test
    void handle_exception_with_unrecognized_property_exception_throws_expected_exception_without_parameter_recommendation() {
        final List<Object> knownPropertyIds = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final String propertyName = UUID.randomUUID().toString();

        final JsonMappingException.Reference firstPathReference = mock(JsonMappingException.Reference.class);
        when(firstPathReference.getFieldName()).thenReturn(UUID.randomUUID().toString());

        final JsonMappingException.Reference secondPathReference = mock(JsonMappingException.Reference.class);
        when(secondPathReference.getFieldName()).thenReturn(UUID.randomUUID().toString());

        final List<JsonMappingException.Reference> path = List.of(firstPathReference, secondPathReference);

        final UnrecognizedPropertyException unrecognizedPropertyException = mock(UnrecognizedPropertyException.class);
        when(unrecognizedPropertyException.getKnownPropertyIds()).thenReturn(knownPropertyIds);
        when(unrecognizedPropertyException.getPropertyName()).thenReturn(propertyName);
        when(unrecognizedPropertyException.getPath()).thenReturn(path);

        when(levenshteinDistance.apply(eq(propertyName), anyString())).thenReturn(10).thenReturn(MIN_DISTANCE_TO_RECOMMEND_PROPERTY + 1);

        final PipelineConfigurationErrorHandler objectUnderTest = createObjectUnderTest();

        final Exception resultingException = objectUnderTest.handleException(unrecognizedPropertyException);
        assertThat(resultingException instanceof InvalidPipelineConfigurationException, equalTo(true));

        final String expectedErrorMessage = String.format(UNRECOGNIZED_PROPERTY_EXCEPTION_FORMAT,
                firstPathReference.getFieldName() + "." + secondPathReference.getFieldName(),
                knownPropertyIds);

        assertThat(resultingException.getMessage(), equalTo(expectedErrorMessage));
    }

    @Test
    void handle_exception_with_unrecognized_property_exception_throws_expected_exception_with_parameter_recommendation() {
        final List<Object> knownPropertyIds = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final String propertyName = UUID.randomUUID().toString();

        final JsonMappingException.Reference firstPathReference = mock(JsonMappingException.Reference.class);
        when(firstPathReference.getFieldName()).thenReturn(UUID.randomUUID().toString());

        final JsonMappingException.Reference secondPathReference = mock(JsonMappingException.Reference.class);
        when(secondPathReference.getFieldName()).thenReturn(UUID.randomUUID().toString());

        final List<JsonMappingException.Reference> path = List.of(firstPathReference, secondPathReference);

        final UnrecognizedPropertyException unrecognizedPropertyException = mock(UnrecognizedPropertyException.class);
        when(unrecognizedPropertyException.getKnownPropertyIds()).thenReturn(knownPropertyIds);
        when(unrecognizedPropertyException.getPropertyName()).thenReturn(propertyName);
        when(unrecognizedPropertyException.getPath()).thenReturn(path);

        when(levenshteinDistance.apply(eq(propertyName), anyString())).thenReturn(10).thenReturn(MIN_DISTANCE_TO_RECOMMEND_PROPERTY - 1);

        final PipelineConfigurationErrorHandler objectUnderTest = createObjectUnderTest();

        final Exception resultingException = objectUnderTest.handleException(unrecognizedPropertyException);
        assertThat(resultingException instanceof InvalidPipelineConfigurationException, equalTo(true));

        String expectedErrorMessage = String.format(UNRECOGNIZED_PROPERTY_EXCEPTION_FORMAT,
                firstPathReference.getFieldName() + "." + secondPathReference.getFieldName(),
                knownPropertyIds);

        expectedErrorMessage += " Did you mean \"" + knownPropertyIds.get(1).toString() + "\"?";



        assertThat(resultingException.getMessage(), equalTo(expectedErrorMessage));
    }

    @Test
    void handle_exception_with_json_mapping_exception_returns_expected_error_message() {
        final JsonMappingException.Reference firstPathReference = mock(JsonMappingException.Reference.class);
        when(firstPathReference.getFieldName()).thenReturn(UUID.randomUUID().toString());

        final JsonMappingException.Reference secondPathReference = mock(JsonMappingException.Reference.class);
        when(secondPathReference.getFieldName()).thenReturn(UUID.randomUUID().toString());

        final List<JsonMappingException.Reference> path = List.of(firstPathReference, secondPathReference);

        final JsonMappingException jsonMappingException = mock(JsonMappingException.class);
        when(jsonMappingException.getPath()).thenReturn(path);
        when(jsonMappingException.getOriginalMessage()).thenReturn(UUID.randomUUID().toString());

        final PipelineConfigurationErrorHandler objectUnderTest = createObjectUnderTest();

        final Exception resultingException = objectUnderTest.handleException(jsonMappingException);
        assertThat(resultingException instanceof InvalidPipelineConfigurationException, equalTo(true));

        final String expectedErrorMessage = String.format(JSON_MAPPING_EXCEPTION_FORMAT,
                firstPathReference.getFieldName() + "." + secondPathReference.getFieldName(),
                jsonMappingException.getOriginalMessage());

        assertThat(resultingException.getMessage(), equalTo(expectedErrorMessage));
    }

    @Test
    void non_handled_exception_throws_generic_invalid_pipeline_configuration_exception() {
        final RuntimeException exception = mock(RuntimeException.class);
        when(exception.getMessage()).thenReturn(UUID.randomUUID().toString());

        final PipelineConfigurationErrorHandler objectUnderTest = createObjectUnderTest();

        final Exception resultingException = objectUnderTest.handleException(exception);
        assertThat(resultingException instanceof InvalidPipelineConfigurationException, equalTo(true));

        final String expectedErrorMessage = exception.getMessage();

        assertThat(resultingException.getMessage(), equalTo(expectedErrorMessage));
    }
}