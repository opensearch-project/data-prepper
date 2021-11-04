package com.amazon.dataprepper.plugin;

import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.plugin.InvalidPluginDefinitionException;
import com.amazon.dataprepper.model.plugin.PluginInvocationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

class PluginCreatorTest {

    private PluginSetting pluginSetting;
    private String pluginName;
    private PluginArgumentsContext pluginConstructionContext;

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

        pluginConstructionContext = mock(PluginArgumentsContext.class);
    }

    private PluginCreator createObjectUnderTest() {
        return new PluginCreator();
    }

    @Test
    void newPluginInstance_should_create_new_instance_from_annotated_constructor() {

        final AlternatePluginConfig alternatePluginConfig = mock(AlternatePluginConfig.class);
        given(pluginConstructionContext.createArguments(new Class[] {PluginSetting.class, AlternatePluginConfig.class}))
                .willReturn(new Object[] { pluginSetting, alternatePluginConfig });

        final PluginClassWithMultipleConstructors instance = createObjectUnderTest()
                .newPluginInstance(PluginClassWithMultipleConstructors.class, pluginConstructionContext, pluginName);

        assertThat(instance, notNullValue());
        assertThat(instance.pluginSetting, equalTo(pluginSetting));
        assertThat(instance.alternatePluginConfig, equalTo(alternatePluginConfig));
    }

    @Test
    void newPluginInstance_should_create_new_instance_from_PluginSetting_if_the_constructor() {
        given(pluginConstructionContext.createArguments(new Class[] {PluginSetting.class}))
                .willReturn(new Object[] { pluginSetting });

        final ValidPluginClass instance = createObjectUnderTest().newPluginInstance(ValidPluginClass.class, pluginConstructionContext, pluginName);

        assertThat(instance, notNullValue());
        assertThat(instance.pluginSetting, equalTo(pluginSetting));
    }

    @Test
    void newPluginInstance_should_create_new_instance_using_default_constructor_if_available() {
        given(pluginConstructionContext.createArguments(new Class[] {PluginSetting.class}))
                .willReturn(new Object[] { pluginSetting });

        final PluginClassWithoutConstructor instance = createObjectUnderTest().newPluginInstance(PluginClassWithoutConstructor.class, pluginConstructionContext, pluginName);

        assertThat(instance, notNullValue());
    }

    @ParameterizedTest
    @ValueSource(classes = {
            InvalidPluginClassDueToUsableConstructor.class,
            InvalidPluginClassDueToMultipleAnnotatedConstructors.class,
            InvalidAbstractPluginClass.class
    })
    void newPluginInstance_should_throw_for_pluginClass_with_invalid_definition(final Class<?> invalidPluginClass) {

        final PluginCreator objectUnderTest = createObjectUnderTest();
        assertThrows(InvalidPluginDefinitionException.class,
                () -> objectUnderTest.newPluginInstance(invalidPluginClass, pluginConstructionContext, pluginName));
    }

    @Test
    void newPluginInstance_should_throw_if_plugin_throws_in_constructor() {
        given(pluginConstructionContext.createArguments(new Class[] {PluginSetting.class}))
                .willReturn(new Object[] { pluginSetting });

        final PluginCreator objectUnderTest = createObjectUnderTest();
        assertThrows(PluginInvocationException.class,
                () -> objectUnderTest.newPluginInstance(AlwaysThrowingPluginClass.class, pluginConstructionContext, pluginName));
    }
}