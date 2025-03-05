/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Path;
import jakarta.validation.Validator;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDeserializationProblemHandler;
import org.opensearch.dataprepper.plugins.test.TestExtension;
import org.opensearch.dataprepper.plugins.test.TestExtensionConfig;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExtensionPluginConfigurationConverterTest {
    @Mock
    private Validator validator;
    @Mock
    private ExtensionPluginConfigurationResolver extensionPluginConfigurationResolver;
    @Mock
    private ConstraintViolation<Object> constraintViolation;

    private final ObjectMapper objectMapper = new ObjectMapperConfiguration()
            .extensionPluginConfigObjectMapper(new DataPrepperDeserializationProblemHandler());
    private ExtensionPluginConfigurationConverter objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new ExtensionPluginConfigurationConverter(
                extensionPluginConfigurationResolver, validator, objectMapper);
    }

    @Test
    void convert_with_null_extensionConfigurationType_should_throw() {
        assertThrows(NullPointerException.class,
                () -> objectUnderTest.convert(true, null, "testKey"));
    }

    @Test
    void convert_with_null_rootKey_should_throw() {
        assertThrows(NullPointerException.class,
                () -> objectUnderTest.convert(true, TestExtension.class, null));
    }

    @Test
    void convert_with_test_extension_with_config_allowed_in_pipeline_configurations() {
        when(validator.validate(any())).thenReturn(Collections.emptySet());
        final String rootKey = "test_extension";
        final String testValue = "test_value";
        when(extensionPluginConfigurationResolver.getCombinedExtensionMap()).thenReturn(Map.of(
                rootKey, Map.of("test_attribute", testValue)
        ));
        final Object testExtensionConfig = objectUnderTest.convert(true, TestExtensionConfig.class, "/" + rootKey);
        assertThat(testExtensionConfig, instanceOf(TestExtensionConfig.class));
        MatcherAssert.assertThat(((TestExtensionConfig) testExtensionConfig).getTestAttribute(), equalTo(testValue));
    }

    @Test
    void convert_with_test_extension_with_config_not_allowed_in_pipeline_configurations() {
        when(validator.validate(any())).thenReturn(Collections.emptySet());
        final String rootKey = "test_extension";
        final String testValue = "test_value";
        when(extensionPluginConfigurationResolver.getDataPrepperConfigExtensionMap()).thenReturn(Map.of(
                rootKey, Map.of("test_attribute", testValue)
        ));
        final Object testExtensionConfig = objectUnderTest.convert(false, TestExtensionConfig.class, "/" + rootKey);
        assertThat(testExtensionConfig, instanceOf(TestExtensionConfig.class));
        MatcherAssert.assertThat(((TestExtensionConfig) testExtensionConfig).getTestAttribute(), equalTo(testValue));
    }

    @Test
    void convert_with_null_rootKey_value_should_return_null() {
        final String rootKeyPath = "/test_extension";
        when(extensionPluginConfigurationResolver.getCombinedExtensionMap()).thenReturn(Collections.emptyMap());
        final Object testExtensionConfig = objectUnderTest.convert(true, TestExtensionConfig.class, rootKeyPath);
        assertThat(testExtensionConfig, nullValue());
    }

    @Test
    void convert_should_throw_exception_when_there_are_constraint_violations() {
        final String firstKey = "first";
        final String secondKey = "second";
        final String jsonPointer = String.format("/%s/%s", firstKey, secondKey);
        when(extensionPluginConfigurationResolver.getCombinedExtensionMap()).thenReturn(
                Map.of(firstKey, Map.of(secondKey, Collections.emptyMap())));
        final String errorMessage = UUID.randomUUID().toString();
        given(constraintViolation.getMessage()).willReturn(errorMessage);
        final String propertyPathString = UUID.randomUUID().toString();
        final Path propertyPath = mock(Path.class);
        given(propertyPath.toString()).willReturn(propertyPathString);
        given(constraintViolation.getPropertyPath()).willReturn(propertyPath);

        given(validator.validate(any()))
                .willReturn(Collections.singleton(constraintViolation));

        final InvalidPluginConfigurationException actualException = assertThrows(InvalidPluginConfigurationException.class,
                () -> objectUnderTest.convert(true, TestExtensionConfig.class, jsonPointer));

        assertThat(actualException.getMessage(), containsString(jsonPointer));
        assertThat(actualException.getMessage(), containsString(propertyPathString));
        assertThat(actualException.getMessage(), containsString(errorMessage));
    }
}