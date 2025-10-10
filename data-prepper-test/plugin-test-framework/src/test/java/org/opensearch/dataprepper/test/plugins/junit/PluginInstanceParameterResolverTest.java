/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.plugins.junit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.test.plugins.DataPrepperPluginTest;
import org.opensearch.dataprepper.test.plugins.PluginConfigurationFile;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Parameter;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PluginInstanceParameterResolverTest {
    @Mock
    private ParameterContext parameterContext;

    @Mock
    private ExtensionContext extensionContext;

    @DataPrepperPluginTest(pluginName = "test_plugin_annotated", pluginType = Processor.class)
    private static class AnnotatedClass {
    }

    private static class UnannotatedClass {
    }

    private PluginInstanceParameterResolver createObjectUnderTest() {
        return new PluginInstanceParameterResolver();
    }

    @ParameterizedTest
    @ValueSource(classes = {UnannotatedClass.class, String.class, Processor.class})
    void supportsParameter_with_unannotated_class_returns_false(final Class<?> unannotatedTestClass) {
        when(extensionContext.getRequiredTestClass()).thenReturn((Class) unannotatedTestClass);

        assertThat(createObjectUnderTest().supportsParameter(parameterContext, extensionContext), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(classes = {UnannotatedClass.class, String.class, Processor.class})
    void resolveParameter_with_unannotated_class_returns_throws(final Class<?> unannotatedTestClass) {
        when(extensionContext.getRequiredTestClass()).thenReturn((Class) unannotatedTestClass);

        final PluginInstanceParameterResolver objectUnderTest = createObjectUnderTest();

        final ParameterResolutionException actualException =
                assertThrows(ParameterResolutionException.class, () -> objectUnderTest.resolveParameter(parameterContext, extensionContext));

        assertThat(actualException.getMessage(), containsString("Missing @DataPrepperPluginTest annotation"));
        assertThat(actualException.getMessage(), containsString(unannotatedTestClass.getName()));
    }

    @Nested
    class WithAnnotatedClass {
        private Class<?> testClass;

        @Mock
        private Parameter parameter;

        @BeforeEach
        void setUp() {
            testClass = AnnotatedClass.class;

            when(extensionContext.getRequiredTestClass()).thenReturn((Class) testClass);
        }

        @Test
        void supportsParameter_returns_true_when_parameterType_is_pluginType() {
            when(parameterContext.getParameter()).thenReturn(parameter);
            when(parameter.getType()).thenReturn((Class) Processor.class);

            assertThat(createObjectUnderTest().supportsParameter(parameterContext, extensionContext), equalTo(true));

        }

        @ParameterizedTest
        @ValueSource(classes = {Sink.class, Source.class, String.class})
        void supportsParameter_returns_false_when_parameterType_is_different_from_the_pluginType(final Class<?> parameterType) {
            when(parameterContext.getParameter()).thenReturn(parameter);
            when(parameter.getType()).thenReturn((Class) parameterType);

            assertThat(createObjectUnderTest().supportsParameter(parameterContext, extensionContext), equalTo(false));
        }

        @Test
        void resolveParameter_throws_exception_when_the_parameter_is_not_annotated_with_a_PluginConfigurationFile_annotation() {
            final PluginInstanceParameterResolver objectUnderTest = createObjectUnderTest();

            when(parameterContext.findAnnotation(PluginConfigurationFile.class)).thenReturn(Optional.empty());

            final ParameterResolutionException actualException = assertThrows(ParameterResolutionException.class, () -> objectUnderTest.resolveParameter(parameterContext, extensionContext));

            assertThat(actualException.getMessage(), containsString("@PluginConfigurationFile"));
        }

        @Test
        void resolveParameter_throws_exception_when_the_parameter_is_annotated_with_a_PluginConfigurationFile_annotation_but_no_file_exists() {
            final PluginInstanceParameterResolver objectUnderTest = createObjectUnderTest();

            final PluginConfigurationFile pluginConfigurationFile = mock(PluginConfigurationFile.class);
            final String configurationFileName = UUID.randomUUID() + ".yaml";
            when(pluginConfigurationFile.value()).thenReturn(configurationFileName);
            when(parameterContext.findAnnotation(PluginConfigurationFile.class)).thenReturn(Optional.of(pluginConfigurationFile));

            final ParameterResolutionException actualException = assertThrows(ParameterResolutionException.class, () -> objectUnderTest.resolveParameter(parameterContext, extensionContext));

            assertThat(actualException.getMessage(), containsString("Unable to find a configuration file"));
            assertThat(actualException.getMessage(), containsString(configurationFileName));
            assertThat(actualException.getMessage(), containsString(testClass.getPackageName()));
        }

        @ParameterizedTest
        @CsvSource({
                "empty.yaml,Failed to parse configuration file",
                "multiple-pipelines.yaml,Test configurations must have exactly one pipeline",
                "no-processor.yaml,define plugins in the processor",
                "multiple-processors.yaml,define plugins in the processor"
        })
        void resolveParameter_throws_exception_when_the_parameter_file_exists_but_is_invalid(
                final String configurationFileName, final String expectedExceptionString) {
            final PluginInstanceParameterResolver objectUnderTest = createObjectUnderTest();

            final PluginConfigurationFile pluginConfigurationFile = mock(PluginConfigurationFile.class);
            when(pluginConfigurationFile.value()).thenReturn(configurationFileName);
            when(parameterContext.findAnnotation(PluginConfigurationFile.class)).thenReturn(Optional.of(pluginConfigurationFile));

            final ParameterResolutionException actualException = assertThrows(ParameterResolutionException.class, () -> objectUnderTest.resolveParameter(parameterContext, extensionContext));

            assertThat(actualException.getMessage(), containsString(expectedExceptionString));
            assertThat(actualException.getMessage(), containsString(configurationFileName));
        }

        @Test
        void resolveParameter_with_valid_configuration_returns_plugin_instance() {
            final PluginInstanceParameterResolver objectUnderTest = createObjectUnderTest();

            final PluginConfigurationFile pluginConfigurationFile = mock(PluginConfigurationFile.class);
            when(pluginConfigurationFile.value()).thenReturn("valid.yaml");
            when(parameterContext.findAnnotation(PluginConfigurationFile.class)).thenReturn(Optional.of(pluginConfigurationFile));

            final ApplicationContext applicationContext = mock(ApplicationContext.class);
            final Object pluginInstance;
            final PluginFactory pluginFactory = mock(PluginFactory.class);
            when(applicationContext.getBean(PluginFactory.class)).thenReturn(pluginFactory);

            final Processor expectedPluginInstance = mock(Processor.class);
            when(pluginFactory.loadPlugin(eq(Processor.class), any(PluginSetting.class)))
                    .thenReturn(expectedPluginInstance);
            try (final MockedStatic<TestApplicationContextProvider> mockedContextProvider = mockStatic(TestApplicationContextProvider.class)) {
                mockedContextProvider.when(() -> TestApplicationContextProvider.get(extensionContext))
                        .thenReturn(applicationContext);
                pluginInstance = objectUnderTest.resolveParameter(parameterContext, extensionContext);
            }

            assertThat(pluginInstance, sameInstance(expectedPluginInstance));

            final ArgumentCaptor<PluginSetting> pluginSettingArgumentCaptor = ArgumentCaptor.forClass(PluginSetting.class);
            verify(pluginFactory).loadPlugin(eq(Processor.class), pluginSettingArgumentCaptor.capture());

            final PluginSetting actualPluginSetting = pluginSettingArgumentCaptor.getValue();

            assertThat(actualPluginSetting.getName(), equalTo("some_test_plugin"));
            assertThat(actualPluginSetting.getSettings(), equalTo(
                    Map.of(
                            "option_a", "some-value-for-a",
                            "option_b", "some-value-for-b",
                            "option_c", "some-value-for-c"
                    )
            ));

            assertThat(actualPluginSetting.getPipelineName(), equalTo("test-pipeline"));
        }
    }
}