package com.amazon.dataprepper.plugin;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.plugin.InvalidPluginDefinitionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

class PluginArgumentsContextTest {

    private PluginSetting pluginSetting;
    private TestPluginConfiguration testPluginConfiguration;

    private static class TestPluginConfiguration { }

    @BeforeEach
    void setUp() {
        pluginSetting = mock(PluginSetting.class);

        testPluginConfiguration = mock(TestPluginConfiguration.class);
    }

    @Test
    void createArguments_with_unavailable_argument_should_throw() {
        final PluginArgumentsContext objectUnderTest = new PluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .build();

        final Class<?>[] parameterTypes = {String.class};
        assertThrows(InvalidPluginDefinitionException.class, () -> objectUnderTest.createArguments(parameterTypes));
    }

    @Test
    void createArguments_with_single_class() {
        final PluginArgumentsContext objectUnderTest = new PluginArgumentsContext.Builder()
                .withPluginConfiguration(testPluginConfiguration)
                .withPluginSetting(pluginSetting)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { TestPluginConfiguration.class }),
                equalTo(new Object[] { testPluginConfiguration}));
    }

    @Test
    void createArguments_with_two_classes() {
        final PluginArgumentsContext objectUnderTest = new PluginArgumentsContext.Builder()
                .withPluginConfiguration(testPluginConfiguration)
                .withPluginSetting(pluginSetting)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { TestPluginConfiguration.class, PluginSetting.class }),
                equalTo(new Object[] { testPluginConfiguration, pluginSetting }));
    }

    @Test
    void createArguments_with_two_classes_inverted_order() {
        final PluginArgumentsContext objectUnderTest = new PluginArgumentsContext.Builder()
                .withPluginConfiguration(testPluginConfiguration)
                .withPluginSetting(pluginSetting)
                .build();

        assertThat(objectUnderTest.createArguments(new Class[] { PluginSetting.class, TestPluginConfiguration.class }),
                equalTo(new Object[] { pluginSetting, testPluginConfiguration }));
    }

    @Test
    void createArguments_with_PluginMetrics() {
        final PluginArgumentsContext objectUnderTest = new PluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .build();

        final PluginMetrics pluginMetrics = mock(PluginMetrics.class);

        final Object[] arguments;
        try(final MockedStatic<PluginMetrics> pluginMetricsMockedStatic = mockStatic(PluginMetrics.class)) {
            pluginMetricsMockedStatic.when(() -> PluginMetrics.fromPluginSetting(pluginSetting))
                    .thenReturn(pluginMetrics);
            arguments = objectUnderTest.createArguments(new Class[]{PluginSetting.class, PluginMetrics.class});
        }
        assertThat(arguments,
                equalTo(new Object[] { pluginSetting, pluginMetrics }));
    }
}