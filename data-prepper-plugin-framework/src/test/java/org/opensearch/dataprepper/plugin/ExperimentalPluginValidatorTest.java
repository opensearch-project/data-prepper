/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.annotations.Experimental;
import org.opensearch.dataprepper.model.plugin.NoPluginFoundException;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExperimentalPluginValidatorTest {

    @Mock
    private ExperimentalConfigurationContainer experimentalConfigurationContainer;

    @Mock
    private ExperimentalConfiguration experimentalConfiguration;

    @Mock
    private DefinedPlugin definedPlugin;

    @BeforeEach
    void setUp() {
        when(experimentalConfigurationContainer.getExperimental()).thenReturn(experimentalConfiguration);
    }

    private ExperimentalPluginValidator createObjectUnderTest() {
        return new ExperimentalPluginValidator(experimentalConfigurationContainer);
    }

    @Test
    void accept_with_non_Experimental_plugin_returns() {
        when(definedPlugin.getPluginClass()).thenReturn(NonExperimentalPlugin.class);

        createObjectUnderTest().accept(definedPlugin);
    }

    @Nested
    class WithExperimentalPlugin {
        @BeforeEach
        void setUp() {
            when(definedPlugin.getPluginClass()).thenReturn(ExperimentalPlugin.class);
        }

        @Test
        void accept_with_Experimental_plugin_throws_if_experimental_is_not_enabled() {
            final String pluginName = UUID.randomUUID().toString();
            when(definedPlugin.getPluginName()).thenReturn(pluginName);

            final ExperimentalPluginValidator objectUnderTest = createObjectUnderTest();

            final NoPluginFoundException actualException = assertThrows(NoPluginFoundException.class, () -> objectUnderTest.accept(definedPlugin));

            assertThat(actualException.getMessage(), notNullValue());
            assertThat(actualException.getMessage(), containsString(pluginName));
            assertThat(actualException.getMessage(), containsString("experimental plugin"));
        }

        @Test
        void accept_with_Experimental_plugin_does_not_throw_if_experimental_is_enabled() {
            when(experimentalConfiguration.isEnableAll()).thenReturn(true);

            createObjectUnderTest().accept(definedPlugin);
        }

        @Test
        void accept_with_Experimental_plugin_throws_if_experimental_is_not_enabled_for_pluginType() {
            final String pluginName = UUID.randomUUID().toString();
            final String pluginTypeName = UUID.randomUUID().toString();
            when(definedPlugin.getPluginName()).thenReturn(pluginName);
            when(definedPlugin.getPluginTypeName()).thenReturn(pluginTypeName);

            final ExperimentalPluginValidator objectUnderTest = createObjectUnderTest();

            final NoPluginFoundException actualException = assertThrows(NoPluginFoundException.class, () -> objectUnderTest.accept(definedPlugin));

            assertThat(actualException.getMessage(), notNullValue());
            assertThat(actualException.getMessage(), containsString(pluginName));
            assertThat(actualException.getMessage(), containsString("experimental plugin"));
        }

        @Test
        void accept_with_Experimental_plugin_throws_if_experimental_is_enabled_for_pluginType_but_not_for_plugin() {
            final String pluginName = UUID.randomUUID().toString();
            final String pluginTypeName = UUID.randomUUID().toString();
            when(definedPlugin.getPluginName()).thenReturn(pluginName);
            when(definedPlugin.getPluginTypeName()).thenReturn(pluginTypeName);
            experimentalConfiguration.getEnabled().put(pluginTypeName, Set.of(UUID.randomUUID().toString()));

            final ExperimentalPluginValidator objectUnderTest = createObjectUnderTest();

            final NoPluginFoundException actualException = assertThrows(NoPluginFoundException.class, () -> objectUnderTest.accept(definedPlugin));

            assertThat(actualException.getMessage(), notNullValue());
            assertThat(actualException.getMessage(), containsString(pluginName));
            assertThat(actualException.getMessage(), containsString("experimental plugin"));
        }

        @Test
        void accept_with_Experimental_plugin_does_not_throw_if_experimental_is_enabled_for_specific_pluginType() {
            final String pluginName = UUID.randomUUID().toString();
            final String pluginTypeName = UUID.randomUUID().toString();
            when(definedPlugin.getPluginName()).thenReturn(pluginName);
            when(definedPlugin.getPluginTypeName()).thenReturn(pluginTypeName);
            when(experimentalConfiguration.getEnabled()).thenReturn(Map.of(pluginTypeName, Set.of(UUID.randomUUID().toString(), pluginName)));

            createObjectUnderTest().accept(definedPlugin);
        }
    }

    private static class NonExperimentalPlugin {
    }

    @Experimental
    private static class ExperimentalPlugin {
    }
}