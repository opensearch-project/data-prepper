/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.schemas.module;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.victools.jsonschema.generator.MemberScope;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.commons.support.ReflectionSupport;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataPrepperJakartaValidationModuleTest {

    @Mock
    private MemberScope memberScope;

    DataPrepperJakartaValidationModule createObjectUnderTest() {
        return spy(new DataPrepperJakartaValidationModule());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void isRequired_returns_inner_if_no_JsonProperty_annotation(final boolean innerIsRequired) throws NoSuchMethodException {
        final DataPrepperJakartaValidationModule objectUnderTest = createObjectUnderTest();

        final boolean isNullable = !innerIsRequired;
        ReflectionSupport.invokeMethod(
                JakartaValidationModule.class.getDeclaredMethod("isNullable", MemberScope.class),
                doReturn(isNullable).when((JakartaValidationModule) objectUnderTest),
                memberScope);

        assertThat(objectUnderTest.isRequired(memberScope), equalTo(innerIsRequired));
    }

    @Nested
    class WithJsonProperty {
        private JsonProperty jsonPropertyAnnotation;

        @BeforeEach
        void setUp() {
            jsonPropertyAnnotation = mock(JsonProperty.class);
            when(memberScope.getAnnotationConsideringFieldAndGetter(JsonProperty.class))
                    .thenReturn(jsonPropertyAnnotation);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void isRequired_returns_inner_if_JsonProperty_annotation_has_null_defaultValue(final boolean innerIsRequired) throws NoSuchMethodException {
            final DataPrepperJakartaValidationModule objectUnderTest = createObjectUnderTest();

            final boolean isNullable = !innerIsRequired;
            ReflectionSupport.invokeMethod(
                    JakartaValidationModule.class.getDeclaredMethod("isNullable", MemberScope.class),
                    doReturn(isNullable).when((JakartaValidationModule) objectUnderTest),
                    memberScope);

            when(jsonPropertyAnnotation.defaultValue()).thenReturn(null);

            assertThat(objectUnderTest.isRequired(memberScope), equalTo(innerIsRequired));
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void isRequired_returns_inner_if_JsonProperty_annotation_has_empty_defaultValue(final boolean innerIsRequired) throws NoSuchMethodException {
            final DataPrepperJakartaValidationModule objectUnderTest = createObjectUnderTest();

            final boolean isNullable = !innerIsRequired;
            ReflectionSupport.invokeMethod(
                    JakartaValidationModule.class.getDeclaredMethod("isNullable", MemberScope.class),
                    doReturn(isNullable).when((JakartaValidationModule) objectUnderTest),
                    memberScope);

            when(jsonPropertyAnnotation.defaultValue()).thenReturn("");

            assertThat(objectUnderTest.isRequired(memberScope), equalTo(innerIsRequired));
        }

        @Test
        void isRequired_returns_false_if_JsonProperty_has_default_value() throws NoSuchMethodException {
            final DataPrepperJakartaValidationModule objectUnderTest = createObjectUnderTest();

            when(jsonPropertyAnnotation.defaultValue()).thenReturn(UUID.randomUUID().toString());

            assertThat(createObjectUnderTest().isRequired(memberScope), equalTo(false));

            ReflectionSupport.invokeMethod(
                    JakartaValidationModule.class.getDeclaredMethod("isNullable", MemberScope.class),
                    verify(objectUnderTest, never()),
                    memberScope);
        }
    }
}