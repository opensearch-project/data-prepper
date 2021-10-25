package com.amazon.dataprepper.plugin;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.plugin.InvalidPluginDefinitionException;
import com.amazon.dataprepper.model.plugin.PluginInvocationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class PluginCreatorTest {

    private PluginSetting pluginSetting;
    private String pluginName;

    public static class ValidPluginClass {
        private final PluginSetting pluginSetting;

        public ValidPluginClass(final PluginSetting pluginSetting) {
            this.pluginSetting = pluginSetting;
        }
    }

    public static class PluginClassWithoutConstructor {
    }

    public abstract static class AbstractPluginClass {
        public AbstractPluginClass(@SuppressWarnings("UnusedParameters") final PluginSetting pluginSetting) { }
    }

    public static class AlwaysThrowingPluginClass {
        public AlwaysThrowingPluginClass(@SuppressWarnings("UnusedParameters") final PluginSetting pluginSetting) {
            throw new RuntimeException("This always throws");
        }
    }

    @BeforeEach
    void setUp() {
        pluginSetting = mock(PluginSetting.class);

        pluginName = UUID.randomUUID().toString();
    }

    private PluginCreator createObjectUnderTest() {
        return new PluginCreator();
    }

    @Test
    void newPluginInstance_should_create_new_instance_from_pluginConfiguration() {

        final ValidPluginClass instance = createObjectUnderTest().newPluginInstance(ValidPluginClass.class, pluginSetting, pluginName);

        assertThat(instance, notNullValue());
        assertThat(instance.pluginSetting, equalTo(pluginSetting));
    }

    @Test
    void newPluginInstance_should_throw_if_no_constructor_with_pluginConfiguration() {

        final PluginCreator objectUnderTest = createObjectUnderTest();
        assertThrows(InvalidPluginDefinitionException.class,
                () -> objectUnderTest.newPluginInstance(PluginClassWithoutConstructor.class, pluginSetting, pluginName));
    }

    @Test
    void newPluginInstance_should_throw_if_plugin_is_abstract() {

        final PluginCreator objectUnderTest = createObjectUnderTest();
        assertThrows(InvalidPluginDefinitionException.class,
                () -> objectUnderTest.newPluginInstance(AbstractPluginClass.class, pluginSetting, pluginName));
    }

    @Test
    void newPluginInstance_should_throw_if_plugin_throws_in_constructor() {

        final PluginCreator objectUnderTest = createObjectUnderTest();
        assertThrows(PluginInvocationException.class,
                () -> objectUnderTest.newPluginInstance(AlwaysThrowingPluginClass.class, pluginSetting, pluginName));
    }
}