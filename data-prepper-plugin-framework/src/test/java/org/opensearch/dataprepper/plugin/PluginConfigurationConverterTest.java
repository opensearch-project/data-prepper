/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Path;
import jakarta.validation.Payload;
import jakarta.validation.Validator;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.metadata.ConstraintDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;

import javax.annotation.Nonnull;
import javax.annotation.meta.When;
import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PluginConfigurationConverterTest {
    private PluginSetting pluginSetting;
    private Validator validator;

    @Mock
    private PluginConfigurationErrorHandler pluginConfigurationErrorHandler;
    private ObjectMapper objectMapper = new ObjectMapper();

    static class TestConfiguration {
        @SuppressWarnings("unused")
        @JsonProperty("my_value")
        private String myValue;

        public String getMyValue() {
            return myValue;
        }
    }

    @BeforeEach
    void setUp() {
        pluginSetting = mock(PluginSetting.class);

        validator = mock(Validator.class);
    }

    private PluginConfigurationConverter createObjectUnderTest() {
        return new PluginConfigurationConverter(validator, objectMapper, pluginConfigurationErrorHandler);
    }

    @Test
    void convert_with_null_configurationType_should_throw() {
        final PluginConfigurationConverter objectUnderTest = createObjectUnderTest();

        assertThrows(NullPointerException.class,
                () -> objectUnderTest.convert(null, pluginSetting));
    }

    @Test
    void convert_with_null_pluginSetting_should_throw() {
        final PluginConfigurationConverter objectUnderTest = createObjectUnderTest();

        assertThrows(NullPointerException.class,
                () -> objectUnderTest.convert(PluginSetting.class, null));
    }

    @Test
    void convert_with_PluginSetting_target_should_return_pluginSetting_object_directly() {
        assertThat(createObjectUnderTest().convert(PluginSetting.class, pluginSetting),
                sameInstance(pluginSetting));

        then(pluginSetting).should().setSettings(anyMap());
        then(validator).shouldHaveNoInteractions();
    }

    @Test
    void convert_with_other_target_should_return_pluginSetting_object_directly() {

        final String value = UUID.randomUUID().toString();
        given(pluginSetting.getSettings())
                .willReturn(Collections.singletonMap("my_value", value));

        final Object convertedConfiguration = createObjectUnderTest().convert(TestConfiguration.class, pluginSetting);

        assertThat(convertedConfiguration, notNullValue());
        assertThat(convertedConfiguration, instanceOf(TestConfiguration.class));

        final TestConfiguration convertedTestConfiguration = (TestConfiguration) convertedConfiguration;

        assertThat(convertedTestConfiguration.getMyValue(), equalTo(value));
    }

    @Test
    void convert_with_other_target_should_return_empty_when_settings_are_null() {
        given(pluginSetting.getSettings())
                .willReturn(null);

        final Object convertedConfiguration = createObjectUnderTest().convert(TestConfiguration.class, pluginSetting);

        assertThat(convertedConfiguration, notNullValue());
        assertThat(convertedConfiguration, instanceOf(TestConfiguration.class));

        final TestConfiguration convertedTestConfiguration = (TestConfiguration) convertedConfiguration;

        assertThat(convertedTestConfiguration.getMyValue(), nullValue());
    }

    @Test
    void convert_with_other_target_should_validate_configuration() {

        final String value = UUID.randomUUID().toString();
        given(pluginSetting.getSettings())
                .willReturn(Collections.singletonMap("my_value", value));

        final Object convertedConfiguration = createObjectUnderTest().convert(TestConfiguration.class, pluginSetting);

        then(validator)
                .should()
                .validate(convertedConfiguration);
    }

    @Test
    void convert_with_other_target_should_throw_exception_when_there_are_constraint_violations_with_for_non_assert_true() {

        final String value = UUID.randomUUID().toString();
        given(pluginSetting.getSettings())
                .willReturn(Collections.singletonMap("my_value", value));

        final String pluginName = UUID.randomUUID().toString();
        given(pluginSetting.getName())
                .willReturn(pluginName);

        final String pipelineName = UUID.randomUUID().toString();
        given(pluginSetting.getPipelineName())
                .willReturn(pipelineName);

        @SuppressWarnings("unchecked") final ConstraintViolation<Object> constraintViolation = mock(ConstraintViolation.class);
        final String errorMessage = UUID.randomUUID().toString();
        given(constraintViolation.getMessage()).willReturn(errorMessage);
        final String propertyPathString = UUID.randomUUID().toString();
        final Path propertyPath = mock(Path.class);
        given(propertyPath.toString()).willReturn(propertyPathString);
        given(constraintViolation.getPropertyPath()).willReturn(propertyPath);

        final ConstraintDescriptor constraintDescriptor = mock(ConstraintDescriptor.class);
        given(constraintViolation.getConstraintDescriptor()).willReturn(constraintDescriptor);

        Nonnull annotation = new Nonnull() {
            @Override
            public When when() {
                return null;
            }

            @Override
            public Class<? extends Annotation> annotationType() {
                return Nonnull.class;
            }
        };

        when(constraintDescriptor.getAnnotation()).thenReturn(annotation);

        given(validator.validate(any()))
                .willReturn(Collections.singleton(constraintViolation));

        final PluginConfigurationConverter objectUnderTest = createObjectUnderTest();

        final InvalidPluginConfigurationException actualException = assertThrows(InvalidPluginConfigurationException.class,
                () -> objectUnderTest.convert(TestConfiguration.class, pluginSetting));

        assertThat(actualException.getMessage(), containsString(pluginName));
        assertThat(actualException.getMessage(), containsString(pipelineName));
        assertThat(actualException.getMessage(), containsString(propertyPathString));
        assertThat(actualException.getMessage(), containsString(errorMessage));
    }

    @Test
    void convert_with_other_target_should_throw_exception_when_there_are_constraint_violations_with_assert_true_annotation() {

        final String value = UUID.randomUUID().toString();
        given(pluginSetting.getSettings())
                .willReturn(Collections.singletonMap("my_value", value));

        final String pluginName = UUID.randomUUID().toString();
        given(pluginSetting.getName())
                .willReturn(pluginName);

        final String pipelineName = UUID.randomUUID().toString();
        given(pluginSetting.getPipelineName())
                .willReturn(pipelineName);

        @SuppressWarnings("unchecked") final ConstraintViolation<Object> constraintViolation = mock(ConstraintViolation.class);
        final String errorMessage = UUID.randomUUID().toString();
        given(constraintViolation.getMessage()).willReturn(errorMessage);

        final ConstraintDescriptor constraintDescriptor = mock(ConstraintDescriptor.class);
        given(constraintViolation.getConstraintDescriptor()).willReturn(constraintDescriptor);

        AssertTrue annotation = new AssertTrue() {
            @Override
            public String message() {
                return null;
            }

            @Override
            public Class<?>[] groups() {
                return new Class[0];
            }

            @Override
            public Class<? extends Payload>[] payload() {
                return new Class[0];
            }

            @Override
            public Class<? extends Annotation> annotationType() {
                return AssertTrue.class;
            }
        };

        when(constraintDescriptor.getAnnotation()).thenReturn(annotation);

        given(validator.validate(any()))
                .willReturn(Collections.singleton(constraintViolation));

        final PluginConfigurationConverter objectUnderTest = createObjectUnderTest();

        final InvalidPluginConfigurationException actualException = assertThrows(InvalidPluginConfigurationException.class,
                () -> objectUnderTest.convert(TestConfiguration.class, pluginSetting));

        assertThat(actualException.getMessage(), containsString(pluginName));
        assertThat(actualException.getMessage(), containsString(pipelineName));
        assertThat(actualException.getMessage(), containsString(errorMessage));
    }

    @Test
    void convert_with_error_when_converting_with_object_mapper_calls_plugin_configuration_error_handler() {
        objectMapper = mock(ObjectMapper.class);

        final String value = UUID.randomUUID().toString();
        given(pluginSetting.getSettings())
                .willReturn(Collections.singletonMap("my_value", value));

        final RuntimeException e = mock(RuntimeException.class);

        when(objectMapper.convertValue(pluginSetting.getSettings(), TestConfiguration.class))
                .thenThrow(e);

        when(pluginConfigurationErrorHandler.handleException(pluginSetting, e)).thenReturn(new IllegalArgumentException());

        final PluginConfigurationConverter objectUnderTest = createObjectUnderTest();

        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.convert(TestConfiguration.class, pluginSetting));
    }

    @Test
    void convert_with_error_when_throws_InvalidPluginConfiguration_when_plugin_configuration_error_handler_does_not_throw() {
        objectMapper = mock(ObjectMapper.class);

        final String value = UUID.randomUUID().toString();
        given(pluginSetting.getSettings())
                .willReturn(Collections.singletonMap("my_value", value));

        final RuntimeException e = mock(RuntimeException.class);

        when(objectMapper.convertValue(pluginSetting.getSettings(), TestConfiguration.class))
                .thenThrow(e);

        when(pluginConfigurationErrorHandler.handleException(pluginSetting, e)).thenReturn(new InvalidPluginConfigurationException(UUID.randomUUID().toString()));

        final PluginConfigurationConverter objectUnderTest = createObjectUnderTest();

        assertThrows(InvalidPluginConfigurationException.class, () -> objectUnderTest.convert(TestConfiguration.class, pluginSetting));
    }
}