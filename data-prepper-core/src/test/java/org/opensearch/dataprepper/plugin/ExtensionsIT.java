/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.acknowledgements.DefaultAcknowledgementSetManager;
import org.opensearch.dataprepper.event.DefaultEventFactory;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.test.TestExtension;
import org.opensearch.dataprepper.plugins.TestPluginUsingExtension;
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

public class ExtensionsIT {
    private AnnotationConfigApplicationContext publicContext;
    private AnnotationConfigApplicationContext coreContext;
    private PluginFactory pluginFactory;
    private String pluginName;
    private String pipelineName;

    @BeforeEach
    void setUp() {
        pluginName = "test_plugin_using_extension";
        pipelineName = UUID.randomUUID().toString();
        publicContext = new AnnotationConfigApplicationContext();
        publicContext.refresh();

        coreContext = new AnnotationConfigApplicationContext();
        coreContext.setParent(publicContext);

        coreContext.scan(DefaultEventFactory.class.getPackage().getName());
        coreContext.scan(DefaultAcknowledgementSetManager.class.getPackage().getName());

        coreContext.scan(DefaultPluginFactory.class.getPackage().getName());
        coreContext.refresh();

        pluginFactory = coreContext.getBean(DefaultPluginFactory.class);
    }

    @AfterEach
    void tearDown() {
        TestExtension.reset();
    }

    @Test
    void applyExtensions_creates_a_single_instance_of_the_extension() {
        assertThat(TestExtension.getConstructedInstances(), equalTo(1));
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
