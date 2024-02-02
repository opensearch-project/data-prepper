package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.InvalidPluginDefinitionException;
import org.opensearch.dataprepper.model.plugin.PluginInvocationException;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

class ExtensionPluginCreatorTest {

    private String pluginName;
    private ComponentPluginArgumentsContext pluginConstructionContext;

    public static class PluginClassWithoutConstructor {
    }

    public static class InvalidPluginClassDueToUsableConstructor {
        public InvalidPluginClassDueToUsableConstructor(final String ignored) {}
    }

    public abstract static class InvalidAbstractPluginClass {
        public InvalidAbstractPluginClass(@SuppressWarnings("UnusedParameters") final Object pluginConfig) { }
    }

    public static class AlwaysThrowingPluginClass {
        public AlwaysThrowingPluginClass() {
            throw new RuntimeException("This always throws");
        }
    }

    public static class AlternatePluginConfig {

    }

    public static class PluginClassWithMultipleConstructors {
        private ExtensionPluginCreatorTest.AlternatePluginConfig alternatePluginConfig;

        public PluginClassWithMultipleConstructors() {}
        public PluginClassWithMultipleConstructors(final String ignored) { }

        @DataPrepperPluginConstructor
        public PluginClassWithMultipleConstructors(final ExtensionPluginCreatorTest.AlternatePluginConfig alternatePluginConfig) {
            this.alternatePluginConfig = alternatePluginConfig;
        }

    }

    public static class PluginClassWithTwoArgs extends ExtensionPluginCreatorTest.PluginClassWithMultipleConstructors {
        private Object obj;
        private ExtensionPluginCreatorTest.AlternatePluginConfig alternatePluginConfig;

        public PluginClassWithTwoArgs() {}
        public PluginClassWithTwoArgs(final String ignored) { }
        @DataPrepperPluginConstructor
        public PluginClassWithTwoArgs(final ExtensionPluginCreatorTest.AlternatePluginConfig alternatePluginConfig, Object obj) {
            this.alternatePluginConfig = alternatePluginConfig;
            this.obj = obj;
        }
    }

    public static class InvalidPluginClassDueToMultipleAnnotatedConstructors {
        @DataPrepperPluginConstructor
        public InvalidPluginClassDueToMultipleAnnotatedConstructors() {}

        @DataPrepperPluginConstructor
        public InvalidPluginClassDueToMultipleAnnotatedConstructors(final Object pluginConfig) {}
    }

    @BeforeEach
    void setUp() {

        pluginName = UUID.randomUUID().toString();

        pluginConstructionContext = mock(ComponentPluginArgumentsContext.class);
    }

    private ExtensionPluginCreator createObjectUnderTest() {
        return new ExtensionPluginCreator();
    }

    @Test
    void newPluginInstance_should_create_new_instance_from_annotated_constructor() {

        final ExtensionPluginCreatorTest.AlternatePluginConfig alternatePluginConfig = mock(
                ExtensionPluginCreatorTest.AlternatePluginConfig.class);
        given(pluginConstructionContext.createArguments(new Class[] {ExtensionPluginCreatorTest.AlternatePluginConfig.class}))
                .willReturn(new Object[] { alternatePluginConfig });

        final ExtensionPluginCreatorTest.PluginClassWithMultipleConstructors instance = createObjectUnderTest()
                .newPluginInstance(ExtensionPluginCreatorTest.PluginClassWithMultipleConstructors.class, pluginConstructionContext, pluginName);

        assertThat(instance, notNullValue());
        assertThat(instance.alternatePluginConfig, equalTo(alternatePluginConfig));
    }

    @Test
    void newPluginInstance_should_create_new_instance_from_annotated_constructor_with_byte_decoder() {

        Object obj = new Object();
        final ExtensionPluginCreatorTest.AlternatePluginConfig alternatePluginConfig = mock(
                ExtensionPluginCreatorTest.AlternatePluginConfig.class);
        given(pluginConstructionContext.createArguments(new Class[] {ExtensionPluginCreatorTest.AlternatePluginConfig.class, Object.class}, obj))
                .willReturn(new Object[] { alternatePluginConfig, obj});

        final PluginClassWithTwoArgs instance = createObjectUnderTest()
                .newPluginInstance(PluginClassWithTwoArgs.class, pluginConstructionContext, pluginName, obj);

        assertThat(instance, notNullValue());
        assertThat(instance.alternatePluginConfig, equalTo(alternatePluginConfig));
        assertThat(instance.obj, equalTo(obj));
    }

    @Test
    void newPluginInstance_should_create_new_instance_using_default_constructor_if_available() {
        given(pluginConstructionContext.createArguments(new Class[] {PluginSetting.class}))
                .willReturn(new Object[0]);

        final ExtensionPluginCreatorTest.PluginClassWithoutConstructor instance = createObjectUnderTest().newPluginInstance(
                ExtensionPluginCreatorTest.PluginClassWithoutConstructor.class, pluginConstructionContext, pluginName);

        assertThat(instance, notNullValue());
    }

    @ParameterizedTest
    @ValueSource(classes = {
            ExtensionPluginCreatorTest.InvalidPluginClassDueToUsableConstructor.class,
            ExtensionPluginCreatorTest.InvalidPluginClassDueToMultipleAnnotatedConstructors.class,
            ExtensionPluginCreatorTest.InvalidAbstractPluginClass.class
    })
    void newPluginInstance_should_throw_for_pluginClass_with_invalid_definition(final Class<?> invalidPluginClass) {

        final ExtensionPluginCreator objectUnderTest = createObjectUnderTest();
        assertThrows(InvalidPluginDefinitionException.class,
                () -> objectUnderTest.newPluginInstance(invalidPluginClass, pluginConstructionContext, pluginName));
    }

    @Test
    void newPluginInstance_should_throw_if_plugin_throws_in_constructor() {
        given(pluginConstructionContext.createArguments(new Class[] {PluginSetting.class}))
                .willReturn(new Object[0]);

        final ExtensionPluginCreator objectUnderTest = createObjectUnderTest();
        assertThrows(PluginInvocationException.class,
                () -> objectUnderTest.newPluginInstance(ExtensionPluginCreatorTest.AlwaysThrowingPluginClass.class, pluginConstructionContext, pluginName));
    }
}