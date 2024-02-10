/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.InvalidPluginDefinitionException;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.plugin.PluginInvocationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class PluginCreatorTest {

    private PluginSetting pluginSetting;
    private String pluginName;
    private ComponentPluginArgumentsContext pluginConstructionContext;
    private PluginConfigurationObservableRegister pluginConfigurationObservableRegister;

    public static class ValidPluginClass {
        private final PluginSetting pluginSetting;

        public ValidPluginClass(final PluginSetting pluginSetting) {
            this.pluginSetting = pluginSetting;
        }
    }

    public static class PluginClassWithoutConstructor {
    }

    public static class InvalidPluginClassDueToUsableConstructor {
        public InvalidPluginClassDueToUsableConstructor(final String ignored) {}
    }

    public abstract static class InvalidAbstractPluginClass {
        public InvalidAbstractPluginClass(@SuppressWarnings("UnusedParameters") final PluginSetting pluginSetting) { }
    }

    public static class AlwaysThrowingPluginClass {
        public AlwaysThrowingPluginClass(@SuppressWarnings("UnusedParameters") final PluginSetting pluginSetting) {
            throw new RuntimeException("This always throws");
        }
    }

    public static class AlternatePluginConfig {

    }

    public static class PluginClassWithMultipleConstructors {
        private PluginSetting pluginSetting;
        private AlternatePluginConfig alternatePluginConfig;

        public PluginClassWithMultipleConstructors() {}
        public PluginClassWithMultipleConstructors(final String ignored) { }

        @DataPrepperPluginConstructor
        public PluginClassWithMultipleConstructors(final PluginSetting pluginSetting, final AlternatePluginConfig alternatePluginConfig) {
            this.pluginSetting = pluginSetting;
            this.alternatePluginConfig = alternatePluginConfig;
        }

    }

    public static class PluginClassWithThreeArgs extends PluginClassWithMultipleConstructors {
        private Object obj;
        private PluginSetting pluginSetting;
        private AlternatePluginConfig alternatePluginConfig;

        public PluginClassWithThreeArgs() {}
        public PluginClassWithThreeArgs(final String ignored) { }
        @DataPrepperPluginConstructor
        public PluginClassWithThreeArgs(final PluginSetting pluginSetting, final AlternatePluginConfig alternatePluginConfig, Object obj) {
            this.pluginSetting = pluginSetting;
            this.alternatePluginConfig = alternatePluginConfig;
            this.obj = obj;
        }
    }

    public static class PluginClassWithPluginConfigurationObservableConstructor {
        private PluginSetting pluginSetting;
        private PluginConfigObservable pluginConfigObservable;

        @DataPrepperPluginConstructor
        public PluginClassWithPluginConfigurationObservableConstructor(
                final PluginSetting pluginSetting, final PluginConfigObservable pluginConfigObservable) {
            this.pluginSetting = pluginSetting;
            this.pluginConfigObservable = pluginConfigObservable;
        }
    }

    public static class InvalidPluginClassDueToMultipleAnnotatedConstructors {
        @DataPrepperPluginConstructor
        public InvalidPluginClassDueToMultipleAnnotatedConstructors() {}

        @DataPrepperPluginConstructor
        public InvalidPluginClassDueToMultipleAnnotatedConstructors(final PluginSetting pluginSetting) {}
    }

    @BeforeEach
    void setUp() {
        pluginSetting = mock(PluginSetting.class);

        pluginName = UUID.randomUUID().toString();

        pluginConstructionContext = mock(ComponentPluginArgumentsContext.class);

        pluginConfigurationObservableRegister = mock(PluginConfigurationObservableRegister.class);
    }

    private PluginCreator createObjectUnderTest(
            final PluginConfigurationObservableRegister pluginConfigurationObservableRegister) {
        return new PluginCreator(pluginConfigurationObservableRegister);
    }

    @ParameterizedTest
    @MethodSource("providePluginConfigurationObservableRegister")
    void newPluginInstance_should_create_new_instance_from_annotated_constructor(
            final PluginConfigurationObservableRegister pluginConfigurationObservableRegister) {

        final AlternatePluginConfig alternatePluginConfig = mock(AlternatePluginConfig.class);
        given(pluginConstructionContext.createArguments(new Class[] {PluginSetting.class, AlternatePluginConfig.class}))
                .willReturn(new Object[] { pluginSetting, alternatePluginConfig });

        final PluginClassWithMultipleConstructors instance = createObjectUnderTest(pluginConfigurationObservableRegister)
                .newPluginInstance(PluginClassWithMultipleConstructors.class, pluginConstructionContext, pluginName);

        assertThat(instance, notNullValue());
        assertThat(instance.pluginSetting, equalTo(pluginSetting));
        assertThat(instance.alternatePluginConfig, equalTo(alternatePluginConfig));
    }

    @ParameterizedTest
    @MethodSource("providePluginConfigurationObservableRegister")
    void newPluginInstance_should_create_new_instance_from_annotated_constructor_with_byte_decoder(
            final PluginConfigurationObservableRegister pluginConfigurationObservableRegister) {

        Object obj = new Object();
        final AlternatePluginConfig alternatePluginConfig = mock(AlternatePluginConfig.class);
        given(pluginConstructionContext.createArguments(new Class[] {PluginSetting.class, AlternatePluginConfig.class, Object.class}, obj))
                .willReturn(new Object[] { pluginSetting, alternatePluginConfig, obj});

        final PluginClassWithThreeArgs instance = createObjectUnderTest(pluginConfigurationObservableRegister)
                .newPluginInstance(PluginClassWithThreeArgs.class, pluginConstructionContext, pluginName, obj);

        assertThat(instance, notNullValue());
        assertThat(instance.pluginSetting, equalTo(pluginSetting));
        assertThat(instance.alternatePluginConfig, equalTo(alternatePluginConfig));
        assertThat(instance.obj, equalTo(obj));
    }

    @Test
    void newPluginInstance_should_register_pluginConfigurationObservable() {
        final PluginCreator objectUnderTest = new PluginCreator(pluginConfigurationObservableRegister);
        final PluginConfigObservable pluginConfigObservable = mock(PluginConfigObservable.class);
        final Object[] constructorArgs = new Object[] { pluginSetting, pluginConfigObservable };
        given(pluginConstructionContext.createArguments(new Class[] {PluginSetting.class, PluginConfigObservable.class}))
                .willReturn(constructorArgs);

        final PluginClassWithPluginConfigurationObservableConstructor instance = objectUnderTest
                .newPluginInstance(PluginClassWithPluginConfigurationObservableConstructor.class, pluginConstructionContext, pluginName);

        verify(pluginConfigurationObservableRegister).registerPluginConfigurationObservables(constructorArgs);
        assertThat(instance, notNullValue());
        assertThat(instance.pluginSetting, equalTo(pluginSetting));
        assertThat(instance.pluginConfigObservable, equalTo(pluginConfigObservable));
    }

    @ParameterizedTest
    @MethodSource("providePluginConfigurationObservableRegister")
    void newPluginInstance_should_create_new_instance_from_PluginSetting_if_the_constructor(
            final PluginConfigurationObservableRegister pluginConfigurationObservableRegister) {
        given(pluginConstructionContext.createArguments(new Class[] {PluginSetting.class}))
                .willReturn(new Object[] { pluginSetting });

        final ValidPluginClass instance = createObjectUnderTest(pluginConfigurationObservableRegister)
                .newPluginInstance(ValidPluginClass.class, pluginConstructionContext, pluginName);

        assertThat(instance, notNullValue());
        assertThat(instance.pluginSetting, equalTo(pluginSetting));
    }

    @ParameterizedTest
    @MethodSource("providePluginConfigurationObservableRegister")
    void newPluginInstance_should_create_new_instance_using_default_constructor_if_available(
            final PluginConfigurationObservableRegister pluginConfigurationObservableRegister) {
        given(pluginConstructionContext.createArguments(new Class[] {PluginSetting.class}))
                .willReturn(new Object[] { pluginSetting });

        final PluginClassWithoutConstructor instance = createObjectUnderTest(pluginConfigurationObservableRegister)
                .newPluginInstance(PluginClassWithoutConstructor.class, pluginConstructionContext, pluginName);

        assertThat(instance, notNullValue());
    }

    @ParameterizedTest
    @MethodSource("providePluginConfigurationObservableRegisterAndInvalidPluginClasses")
    void newPluginInstance_should_throw_for_pluginClass_with_invalid_definition(
            final PluginConfigurationObservableRegister pluginConfigurationObservableRegister,
            final Class<?> invalidPluginClass) {

        final PluginCreator objectUnderTest = createObjectUnderTest(pluginConfigurationObservableRegister);
        assertThrows(InvalidPluginDefinitionException.class,
                () -> objectUnderTest.newPluginInstance(invalidPluginClass, pluginConstructionContext, pluginName));
    }

    @ParameterizedTest
    @MethodSource("providePluginConfigurationObservableRegister")
    void newPluginInstance_should_throw_if_plugin_throws_in_constructor(
            final PluginConfigurationObservableRegister pluginConfigurationObservableRegister) {
        given(pluginConstructionContext.createArguments(new Class[] {PluginSetting.class}))
                .willReturn(new Object[] { pluginSetting });

        final PluginCreator objectUnderTest = createObjectUnderTest(pluginConfigurationObservableRegister);
        assertThrows(PluginInvocationException.class,
                () -> objectUnderTest.newPluginInstance(AlwaysThrowingPluginClass.class, pluginConstructionContext, pluginName));
    }

    private static Stream<Arguments> providePluginConfigurationObservableRegister() {
        return Stream.of(
                null,
                Arguments.of(mock(PluginConfigurationObservableRegister.class))
        );
    }

    private static Stream<Object[]> providePluginConfigurationObservableRegisterAndInvalidPluginClasses() {
        return Stream.of(
                new Object[]{null, InvalidPluginClassDueToUsableConstructor.class},
                new Object[]{null, InvalidPluginClassDueToMultipleAnnotatedConstructors.class},
                new Object[]{null, InvalidAbstractPluginClass.class},
                new Object[]{mock(PluginConfigurationObservableRegister.class), InvalidPluginClassDueToUsableConstructor.class},
                new Object[]{mock(PluginConfigurationObservableRegister.class), InvalidPluginClassDueToMultipleAnnotatedConstructors.class},
                new Object[]{mock(PluginConfigurationObservableRegister.class), InvalidAbstractPluginClass.class}
        );
    }
}
