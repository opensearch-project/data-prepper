/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.acknowledgements.DefaultAcknowledgementSetManager;
import org.opensearch.dataprepper.core.event.EventFactoryApplicationContextMarker;
import org.opensearch.dataprepper.core.validation.LoggingPluginErrorsHandler;
import org.opensearch.dataprepper.core.validation.PluginErrorCollector;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.NoPluginFoundException;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDeserializationProblemHandler;
import org.opensearch.dataprepper.plugins.TestObjectPlugin;
import org.opensearch.dataprepper.plugins.configtest.TestComponentWithConfigInject;
import org.opensearch.dataprepper.plugins.configtest.TestDISourceWithConfig;
import org.opensearch.dataprepper.plugins.test.TestComponent;
import org.opensearch.dataprepper.plugins.test.TestDISource;
import org.opensearch.dataprepper.plugins.test.TestPlugin;
import org.opensearch.dataprepper.validation.PluginErrorsHandler;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

/**
 * Integration test of the plugin framework. These tests should not mock any portion
 * of the plugin framework. But, they may mock inputs when appropriate.
 */
@ExtendWith(MockitoExtension.class)
class DefaultPluginFactoryIT {
    @Mock
    private PipelinesDataFlowModel pipelinesDataFlowModel;
    @Mock
    private ExtensionsConfiguration extensionsConfiguration;
    @Mock
    private ExperimentalConfigurationContainer experimentalConfigurationContainer;
    private String pluginName;
    private String objectPluginName;
    private String pipelineName;

    @BeforeEach
    void setUp() {
        pluginName = "test_plugin";
        objectPluginName = "test_object_plugin";
        pipelineName = UUID.randomUUID().toString();
    }

    private DefaultPluginFactory createObjectUnderTest() {
        final AnnotationConfigApplicationContext publicContext = new AnnotationConfigApplicationContext();
        publicContext.refresh();

        final AnnotationConfigApplicationContext coreContext = new AnnotationConfigApplicationContext();
        coreContext.setParent(publicContext);

        when(experimentalConfigurationContainer.getExperimental()).thenReturn(ExperimentalConfiguration.defaultConfiguration());

        coreContext.scan(EventFactoryApplicationContextMarker.class.getPackage().getName());
        coreContext.scan(DefaultAcknowledgementSetManager.class.getPackage().getName());
        coreContext.scan(DefaultPluginFactory.class.getPackage().getName());
        coreContext.register(PluginBeanFactoryProvider.class);
        coreContext.registerBean(PluginErrorCollector.class, PluginErrorCollector::new);
        coreContext.registerBean(PluginErrorsHandler.class, LoggingPluginErrorsHandler::new);
        coreContext.registerBean(DataPrepperDeserializationProblemHandler.class, DataPrepperDeserializationProblemHandler::new);
        coreContext.registerBean(ExtensionsConfiguration.class, () -> extensionsConfiguration);
        coreContext.registerBean(PipelinesDataFlowModel.class, () -> pipelinesDataFlowModel);
        coreContext.registerBean(ExperimentalConfigurationContainer.class, () -> experimentalConfigurationContainer);
        coreContext.refresh();

        return coreContext.getBean(DefaultPluginFactory.class);
    }

    @Test
    void loadPlugin_should_return_a_new_plugin_instance_with_the_expected_configuration() {

        final String requiredStringValue = UUID.randomUUID().toString();
        final String optionalStringValue = UUID.randomUUID().toString();

        final Map<String, Object> pluginSettingMap = new HashMap<>();
        pluginSettingMap.put("required_string", requiredStringValue);
        pluginSettingMap.put("optional_string", optionalStringValue);
        final PluginSetting pluginSetting = createPluginSettings(pluginSettingMap);

        final TestPluggableInterface plugin = createObjectUnderTest().loadPlugin(TestPluggableInterface.class, pluginSetting);

        assertThat(plugin, instanceOf(TestPlugin.class));

        final TestPlugin testPlugin = (TestPlugin) plugin;

        final TestPluginConfiguration configuration = testPlugin.getConfiguration();

        assertThat(configuration.getRequiredString(), equalTo(requiredStringValue));
        assertThat(configuration.getOptionalString(), equalTo(optionalStringValue));
    }

    @Test
    void loadPlugin_should_return_a_new_plugin_instance_with_DI_context_initialized() {

        final Map<String, Object> pluginSettingMap = new HashMap<>();
        final PluginSetting pluginSetting = new PluginSetting("test_di_source", pluginSettingMap);
        pluginSetting.setPipelineName(pipelineName);

        final Source sourcePlugin = createObjectUnderTest().loadPlugin(Source.class, pluginSetting);

        assertThat(sourcePlugin, instanceOf(TestDISource.class));
        TestDISource plugin = (TestDISource) sourcePlugin;
        // Testing the auto wired been with the Dependency Injection
        assertNotNull(plugin.getTestComponent());
        assertInstanceOf(TestComponent.class, plugin.getTestComponent());
        assertThat(plugin.getTestComponent().getIdentifier(), equalTo("test-component"));
    }

    @Test
    void loadPlugin_should_return_a_new_plugin_instance_with_DI_context_with_config_and_plugin_metrics_injected() {

        final String requiredStringValue = UUID.randomUUID().toString();
        final String optionalStringValue = UUID.randomUUID().toString();

        final Map<String, Object> pluginSettingMap = new HashMap<>();
        pluginSettingMap.put("required_string", requiredStringValue);
        pluginSettingMap.put("optional_string", optionalStringValue);
        final PluginSetting pluginSetting = new PluginSetting("test_di_source_with_config", pluginSettingMap);
        pluginSetting.setPipelineName("test_di_source_with_config");

        final Source sourcePlugin = createObjectUnderTest().loadPlugin(Source.class, pluginSetting);

        assertThat(sourcePlugin, instanceOf(TestDISourceWithConfig.class));
        TestDISourceWithConfig plugin = (TestDISourceWithConfig) sourcePlugin;
        // Testing the auto wired been with the Dependency Injection
        assertNotNull(plugin.getTestComponent());
        assertInstanceOf(TestComponentWithConfigInject.class, plugin.getTestComponent());
        TestPluginConfiguration pluginConfig = plugin.getTestComponent().getConfiguration();
        assertInstanceOf(TestPluginConfiguration.class, pluginConfig);
        assertThat(pluginConfig.getRequiredString(), equalTo(requiredStringValue));
        assertThat(pluginConfig.getOptionalString(), equalTo(optionalStringValue));
        assertThat(plugin.getTestComponent().getIdentifier(), equalTo("test-component-with-plugin-config-injected"));
        PluginMetrics pluginMetrics = plugin.getTestComponent().getPluginMetrics();
        assertInstanceOf(PluginMetrics.class, pluginMetrics);
        assertInstanceOf(Counter.class, pluginMetrics.counter("testCounter"));
    }

    @Test
    void loadPlugin_should_return_a_new_plugin_instance_with_the_expected_configuration_variable_args() {

        final String requiredStringValue = UUID.randomUUID().toString();
        final String optionalStringValue = UUID.randomUUID().toString();

        final Map<String, Object> pluginSettingMap = new HashMap<>();
        pluginSettingMap.put("required_string", requiredStringValue);
        pluginSettingMap.put("optional_string", optionalStringValue);
        final PluginSetting pluginSetting = createObjectPluginSettings(pluginSettingMap);

        final Object object = new Object();
        final TestPluggableInterface plugin = createObjectUnderTest().loadPlugin(TestPluggableInterface.class, pluginSetting, object);

        assertThat(plugin, instanceOf(TestObjectPlugin.class));

        final TestObjectPlugin testPlugin = (TestObjectPlugin) plugin;

        final TestPluginConfiguration configuration = testPlugin.getConfiguration();

        assertThat(testPlugin.getObject(), equalTo(object));
        assertThat(configuration.getRequiredString(), equalTo(requiredStringValue));
        assertThat(configuration.getOptionalString(), equalTo(optionalStringValue));
    }

    @Test
    void loadPlugin_should_throw_when_a_plugin_configuration_is_invalid() {
        final String optionalStringValue = UUID.randomUUID().toString();

        final Map<String, Object> pluginSettingMap = new HashMap<>();
        pluginSettingMap.put("optional_string", optionalStringValue);
        final PluginSetting pluginSetting = createPluginSettings(pluginSettingMap);

        final DefaultPluginFactory objectUnderTest = createObjectUnderTest();

        final InvalidPluginConfigurationException actualException = assertThrows(InvalidPluginConfigurationException.class,
                () -> objectUnderTest.loadPlugin(TestPluggableInterface.class, pluginSetting));

        assertThat(actualException.getMessage(), notNullValue());
        assertThat(actualException.getMessage(), equalTo("Plugin test_plugin in pipeline " + pipelineName + " is configured incorrectly: requiredString must not be null"));
    }

    @Test
    void loadPlugin_should_throw_when_a_plugin_is_experimental_by_default() {
        pluginName = "test_experimental_plugin";
        final PluginSetting pluginSetting = createPluginSettings(Collections.emptyMap());

        final DefaultPluginFactory objectUnderTest = createObjectUnderTest();

        final NoPluginFoundException actualException = assertThrows(NoPluginFoundException.class,
                () -> objectUnderTest.loadPlugin(TestPluggableInterface.class, pluginSetting));

        assertThat(actualException.getMessage(), notNullValue());
        assertThat(actualException.getMessage(), equalTo("Unable to create experimental plugin test_experimental_plugin. You must enable experimental plugins in data-prepper-config.yaml in order to use them."));
    }

    private PluginSetting createPluginSettings(final Map<String, Object> pluginSettingMap) {
        final PluginSetting pluginSetting = new PluginSetting(pluginName, pluginSettingMap);
        pluginSetting.setPipelineName(pipelineName);
        return pluginSetting;
    }

    private PluginSetting createObjectPluginSettings(final Map<String, Object> pluginSettingMap) {
        final PluginSetting pluginSetting = new PluginSetting(objectPluginName, pluginSettingMap);
        pluginSetting.setPipelineName(pipelineName);
        return pluginSetting;
    }
}
