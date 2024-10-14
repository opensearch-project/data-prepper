package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;

import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugin.PluginConfigurationErrorHandler.GENERIC_PLUGIN_EXCEPTION_FORMAT;
import static org.opensearch.dataprepper.plugin.PluginConfigurationErrorHandler.JSON_MAPPING_EXCEPTION_FORMAT;
import static org.opensearch.dataprepper.plugin.PluginConfigurationErrorHandler.MIN_DISTANCE_TO_RECOMMEND_PROPERTY;
import static org.opensearch.dataprepper.plugin.PluginConfigurationErrorHandler.UNRECOGNIZED_PROPERTY_EXCEPTION_FORMAT;

@ExtendWith(MockitoExtension.class)
public class PluginConfigurationErrorHandlerTest {

    @Mock
    private LevenshteinDistance levenshteinDistance;

    private PluginConfigurationErrorHandler createObjectUnderTest() {
        return new PluginConfigurationErrorHandler(levenshteinDistance);
    }

    @Test
    void handle_exception_with_unrecognized_property_exception_throws_expected_exception_without_parameter_recommendation() {
        final List<Object> knownPropertyIds = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final String pluginName = UUID.randomUUID().toString();
        final String propertyName = UUID.randomUUID().toString();

        final JsonMappingException.Reference firstPathReference = mock(JsonMappingException.Reference.class);
        when(firstPathReference.getFieldName()).thenReturn(UUID.randomUUID().toString());

        final JsonMappingException.Reference secondPathReference = mock(JsonMappingException.Reference.class);
        when(secondPathReference.getFieldName()).thenReturn(UUID.randomUUID().toString());

        final List<JsonMappingException.Reference> path = List.of(firstPathReference, secondPathReference);

        final PluginSetting pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getName()).thenReturn(pluginName);

        final RuntimeException exception = mock(RuntimeException.class);

        final UnrecognizedPropertyException unrecognizedPropertyException = mock(UnrecognizedPropertyException.class);
        when(unrecognizedPropertyException.getKnownPropertyIds()).thenReturn(knownPropertyIds);
        when(unrecognizedPropertyException.getPropertyName()).thenReturn(propertyName);
        when(unrecognizedPropertyException.getPath()).thenReturn(path);

        when(exception.getCause()).thenReturn(unrecognizedPropertyException);

        when(levenshteinDistance.apply(eq(propertyName), anyString())).thenReturn(10).thenReturn(MIN_DISTANCE_TO_RECOMMEND_PROPERTY + 1);

        final PluginConfigurationErrorHandler objectUnderTest = createObjectUnderTest();

        final Exception resultingException = objectUnderTest.handleException(pluginSetting, exception);
        assertThat(resultingException instanceof InvalidPluginConfigurationException, equalTo(true));

        final String expectedErrorMessage = String.format(UNRECOGNIZED_PROPERTY_EXCEPTION_FORMAT,
                firstPathReference.getFieldName() + "." + secondPathReference.getFieldName(),
                pluginName,
                knownPropertyIds);

        assertThat(resultingException.getMessage(), equalTo(expectedErrorMessage));
    }

    @Test
    void handle_exception_with_unrecognized_property_exception_throws_expected_exception_with_parameter_recommendation() {
        final List<Object> knownPropertyIds = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final String pluginName = UUID.randomUUID().toString();
        final String propertyName = UUID.randomUUID().toString();

        final JsonMappingException.Reference firstPathReference = mock(JsonMappingException.Reference.class);
        when(firstPathReference.getFieldName()).thenReturn(UUID.randomUUID().toString());

        final JsonMappingException.Reference secondPathReference = mock(JsonMappingException.Reference.class);
        when(secondPathReference.getFieldName()).thenReturn(UUID.randomUUID().toString());

        final List<JsonMappingException.Reference> path = List.of(firstPathReference, secondPathReference);

        final PluginSetting pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getName()).thenReturn(pluginName);

        final RuntimeException exception = mock(RuntimeException.class);

        final UnrecognizedPropertyException unrecognizedPropertyException = mock(UnrecognizedPropertyException.class);
        when(unrecognizedPropertyException.getKnownPropertyIds()).thenReturn(knownPropertyIds);
        when(unrecognizedPropertyException.getPropertyName()).thenReturn(propertyName);
        when(unrecognizedPropertyException.getPath()).thenReturn(path);

        when(exception.getCause()).thenReturn(unrecognizedPropertyException);

        when(levenshteinDistance.apply(eq(propertyName), anyString())).thenReturn(10).thenReturn(MIN_DISTANCE_TO_RECOMMEND_PROPERTY - 1);

        final PluginConfigurationErrorHandler objectUnderTest = createObjectUnderTest();

        final Exception resultingException = objectUnderTest.handleException(pluginSetting, exception);
        assertThat(resultingException instanceof InvalidPluginConfigurationException, equalTo(true));

        String expectedErrorMessage = String.format(UNRECOGNIZED_PROPERTY_EXCEPTION_FORMAT,
                firstPathReference.getFieldName() + "." + secondPathReference.getFieldName(),
                pluginName,
                knownPropertyIds);

        expectedErrorMessage += " Did you mean \"" + knownPropertyIds.get(1).toString() + "\"?";



        assertThat(resultingException.getMessage(), equalTo(expectedErrorMessage));
    }

    @Test
    void handle_exception_with_json_mapping_exception_returns_expected_error_message() {
        final String pluginName = UUID.randomUUID().toString();

        final JsonMappingException.Reference firstPathReference = mock(JsonMappingException.Reference.class);
        when(firstPathReference.getFieldName()).thenReturn(UUID.randomUUID().toString());

        final JsonMappingException.Reference secondPathReference = mock(JsonMappingException.Reference.class);
        when(secondPathReference.getFieldName()).thenReturn(UUID.randomUUID().toString());

        final List<JsonMappingException.Reference> path = List.of(firstPathReference, secondPathReference);

        final PluginSetting pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getName()).thenReturn(pluginName);

        final RuntimeException exception = mock(RuntimeException.class);

        final JsonMappingException jsonMappingException = mock(JsonMappingException.class);
        when(jsonMappingException.getPath()).thenReturn(path);
        when(jsonMappingException.getOriginalMessage()).thenReturn(UUID.randomUUID().toString());

        when(exception.getCause()).thenReturn(jsonMappingException);

        final PluginConfigurationErrorHandler objectUnderTest = createObjectUnderTest();

        final Exception resultingException = objectUnderTest.handleException(pluginSetting, exception);
        assertThat(resultingException instanceof InvalidPluginConfigurationException, equalTo(true));

        final String expectedErrorMessage = String.format(JSON_MAPPING_EXCEPTION_FORMAT,
                firstPathReference.getFieldName() + "." + secondPathReference.getFieldName(),
                pluginName,
                jsonMappingException.getOriginalMessage());

        assertThat(resultingException.getMessage(), equalTo(expectedErrorMessage));
    }

    @Test
    void non_handled_exception_throws_generic_invalid_plugin_configuration_exception() {
        final String pluginName = UUID.randomUUID().toString();

        final PluginSetting pluginSetting = mock(PluginSetting.class);
        when(pluginSetting.getName()).thenReturn(pluginName);

        final RuntimeException exception = mock(RuntimeException.class);
        when(exception.getMessage()).thenReturn(UUID.randomUUID().toString());

        final RuntimeException cause = mock(RuntimeException.class);
        when(exception.getCause()).thenReturn(cause);

        final PluginConfigurationErrorHandler objectUnderTest = createObjectUnderTest();

        final Exception resultingException = objectUnderTest.handleException(pluginSetting, exception);
        assertThat(resultingException instanceof InvalidPluginConfigurationException, equalTo(true));

        final String expectedErrorMessage = String.format(GENERIC_PLUGIN_EXCEPTION_FORMAT,
                pluginName,
                exception.getMessage());

        assertThat(resultingException.getMessage(), equalTo(expectedErrorMessage));
    }
}
