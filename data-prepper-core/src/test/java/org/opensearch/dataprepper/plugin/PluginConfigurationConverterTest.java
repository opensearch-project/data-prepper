/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Path;
import jakarta.validation.Validator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

class PluginConfigurationConverterTest {
    private PluginSetting pluginSetting;
    private Validator validator;
    private final ObjectMapper objectMapper = new ObjectMapperConfiguration().extensionPluginConfigObjectMapper();

    static class TestConfiguration {
        @SuppressWarnings("unused")
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
        return new PluginConfigurationConverter(validator, objectMapper);
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

        then(pluginSetting).shouldHaveNoInteractions();
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
    void convert_with_other_target_should_throw_exception_when_there_are_constraint_violations() {

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
}