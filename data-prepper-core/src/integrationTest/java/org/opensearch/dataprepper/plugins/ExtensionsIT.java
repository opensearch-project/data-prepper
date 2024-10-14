/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.acknowledgements.DefaultAcknowledgementSetManager;
import org.opensearch.dataprepper.core.breaker.CircuitBreakerManager;
import org.opensearch.dataprepper.core.event.EventFactoryApplicationContextMarker;
import org.opensearch.dataprepper.core.parser.config.DataPrepperAppConfiguration;
import org.opensearch.dataprepper.core.parser.config.FileStructurePathProvider;
import org.opensearch.dataprepper.core.parser.config.PipelineParserConfiguration;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderProvider;
import org.opensearch.dataprepper.core.pipeline.router.RouterFactory;
import org.opensearch.dataprepper.core.sourcecoordination.SourceCoordinatorFactory;
import org.opensearch.dataprepper.core.validation.LoggingPluginErrorsHandler;
import org.opensearch.dataprepper.core.validation.PluginErrorCollector;
import org.opensearch.dataprepper.core.validation.PluginErrorsHandler;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugin.DefaultPluginFactory;
import org.opensearch.dataprepper.plugin.ObjectMapperConfiguration;
import org.opensearch.dataprepper.plugin.TestPluggableInterface;
import org.opensearch.dataprepper.plugins.test.TestExtension;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ExtensionsIT {
    @Mock
    private FileStructurePathProvider fileStructurePathProvider;
    @Mock
    private PeerForwarderProvider peerForwarderProvider;
    @Mock
    private RouterFactory routerFactory;
    @Mock
    private CircuitBreakerManager circuitBreakerManager;
    @Mock
    private EventFactory eventFactory;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private SourceCoordinatorFactory sourceCoordinatorFactory;
    private AnnotationConfigApplicationContext publicContext;
    private AnnotationConfigApplicationContext coreContext;
    private PluginFactory pluginFactory;
    private PluginErrorCollector pluginErrorCollector;
    private PluginErrorsHandler pluginErrorsHandler;
    private String pluginName;
    private String pipelineName;

    @BeforeEach
    void setUp() {
        pluginName = "test_plugin_using_extension";
        pipelineName = UUID.randomUUID().toString();
        pluginErrorCollector = new PluginErrorCollector();
        pluginErrorsHandler = new LoggingPluginErrorsHandler();
        publicContext = new AnnotationConfigApplicationContext();
        publicContext.refresh();

        coreContext = new AnnotationConfigApplicationContext();
        coreContext.setParent(publicContext);

        coreContext.scan(EventFactoryApplicationContextMarker.class.getPackage().getName());
        coreContext.scan(DefaultAcknowledgementSetManager.class.getPackage().getName());

        coreContext.scan(DefaultPluginFactory.class.getPackage().getName());

        when(fileStructurePathProvider.getPipelineConfigFileLocation()).thenReturn(
                "src/test/resources/valid_pipeline.yml"
        );
        when(fileStructurePathProvider.getDataPrepperConfigFileLocation()).thenReturn(
                "src/test/resources/valid_data_prepper_config_with_test_extension.yml"
        );

        coreContext.registerBean(FileStructurePathProvider.class, () -> fileStructurePathProvider);
        coreContext.registerBean(PeerForwarderProvider.class, () -> peerForwarderProvider);
        coreContext.registerBean(RouterFactory.class, () -> routerFactory);
        coreContext.registerBean(DataPrepperAppConfiguration.class, DataPrepperAppConfiguration::new);
        coreContext.registerBean(CircuitBreakerManager.class, () -> circuitBreakerManager);
        coreContext.registerBean(EventFactory.class, () -> eventFactory);
        coreContext.registerBean(AcknowledgementSetManager.class, () -> acknowledgementSetManager);
        coreContext.registerBean(SourceCoordinatorFactory.class, () -> sourceCoordinatorFactory);
        coreContext.registerBean(ObjectMapperConfiguration.class, ObjectMapperConfiguration::new);
        coreContext.registerBean(ObjectMapper.class, () -> new ObjectMapper(new YAMLFactory()));
        coreContext.register(PipelineParserConfiguration.class);
        coreContext.registerBean(PluginErrorCollector.class, () -> pluginErrorCollector);
        coreContext.registerBean(PluginErrorsHandler.class, () -> pluginErrorsHandler);
        coreContext.refresh();

        pluginFactory = coreContext.getBean(DefaultPluginFactory.class);
    }

    @AfterEach
    void tearDown() {
        TestExtension.reset();
    }

    @Test
    void applyExtensions_creates_a_single_instance_of_the_extension() {
        MatcherAssert.assertThat(TestExtension.getConstructedInstances(), equalTo(1));
    }

    @Test
    void creating_a_plugin_using_an_extension() {
        final String requiredStringValue = UUID.randomUUID().toString();
        final String optionalStringValue = UUID.randomUUID().toString();
        final Map<String, Object> pluginSettingMap = new HashMap<>();
        pluginSettingMap.put("required_string", requiredStringValue);
        pluginSettingMap.put("optional_string", optionalStringValue);
        final PluginSetting pluginSetting = createPluginSettings(pluginSettingMap);

        final TestPluggableInterface pluggableInterface = pluginFactory.loadPlugin(TestPluggableInterface.class, pluginSetting);

        assertThat(pluggableInterface, notNullValue());
        assertThat(pluggableInterface, instanceOf(TestPluginUsingExtension.class));
        final TestPluginUsingExtension testPluginUsingExtension = (TestPluginUsingExtension) pluggableInterface;
        assertThat(testPluginUsingExtension.getExtensionModel(), notNullValue());
        assertThat(testPluginUsingExtension.getExtensionModel().getExtensionId(), notNullValue());
    }

    @Test
    void creating_a_plugin_using_an_extension_with_config() {
        pluginName = "test_plugin_using_extension_with_config";
        final String requiredStringValue = UUID.randomUUID().toString();
        final String optionalStringValue = UUID.randomUUID().toString();
        final Map<String, Object> pluginSettingMap = new HashMap<>();
        pluginSettingMap.put("required_string", requiredStringValue);
        pluginSettingMap.put("optional_string", optionalStringValue);
        final PluginSetting pluginSetting = createPluginSettings(pluginSettingMap);

        final TestPluggableInterface pluggableInterface = pluginFactory.loadPlugin(TestPluggableInterface.class, pluginSetting);

        assertThat(pluggableInterface, notNullValue());
        assertThat(pluggableInterface, instanceOf(TestPluginUsingExtensionWithConfig.class));
        final TestPluginUsingExtensionWithConfig testPluginUsingExtensionWithConfig =
                (TestPluginUsingExtensionWithConfig) pluggableInterface;
        assertThat(testPluginUsingExtensionWithConfig.getExtensionModel(), notNullValue());
        assertThat(testPluginUsingExtensionWithConfig.getExtensionModel().getTestAttribute(), equalTo("test_string"));
    }

    @Test
    void creating_multiple_plugins_using_an_extension() {
        final String requiredStringValue = UUID.randomUUID().toString();
        final String optionalStringValue = UUID.randomUUID().toString();
        final Map<String, Object> pluginSettingMap = new HashMap<>();
        pluginSettingMap.put("required_string", requiredStringValue);
        pluginSettingMap.put("optional_string", optionalStringValue);
        final PluginSetting pluginSetting = createPluginSettings(pluginSettingMap);

        final Set<String> extensionIds = new HashSet<>();
        final List<TestExtension.TestModel> models = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final TestPluggableInterface pluggableInterface = pluginFactory.loadPlugin(TestPluggableInterface.class, pluginSetting);

            assertThat(pluggableInterface, notNullValue());
            assertThat(pluggableInterface, instanceOf(TestPluginUsingExtension.class));
            final TestPluginUsingExtension testPluginUsingExtension = (TestPluginUsingExtension) pluggableInterface;
            assertThat(testPluginUsingExtension.getExtensionModel(), notNullValue());
            assertThat(testPluginUsingExtension.getExtensionModel().getExtensionId(), notNullValue());

            extensionIds.add(testPluginUsingExtension.getExtensionModel().getExtensionId());

            models.add(testPluginUsingExtension.getExtensionModel());
        }

        assertThat(extensionIds.size(), equalTo(1));

        assertThat(models.size(), equalTo(5));
        for (int i = 0; i < models.size(); i++) {
            for (int j = 0; j < models.size(); j++) {
                if (i != j) {
                    final TestExtension.TestModel model = models.get(i);
                    final TestExtension.TestModel otherModel = models.get(j);
                    assertThat(model, not(sameInstance(otherModel)));
                }
            }
        }
    }

    private PluginSetting createPluginSettings(final Map<String, Object> pluginSettingMap) {
        final PluginSetting pluginSetting = new PluginSetting(pluginName, pluginSettingMap);
        pluginSetting.setPipelineName(pipelineName);
        return pluginSetting;
    }
}
