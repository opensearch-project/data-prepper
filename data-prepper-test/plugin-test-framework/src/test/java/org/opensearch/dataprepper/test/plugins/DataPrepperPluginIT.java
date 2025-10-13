/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.plugins;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.plugins.test.TestPluggableInterface;
import org.opensearch.dataprepper.test.plugins.junit.BaseDataPrepperPluginStandardTestSuite;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@DataPrepperPluginTest(pluginName = "test_plugin", pluginType = TestPluggableInterface.class)
class DataPrepperPluginIT extends BaseDataPrepperPluginStandardTestSuite {
    @Test
    void parameter_for_plugin_instance_from_configuration_provider_gets_the_correct_plugin_instance(@PluginConfigurationFile("test001.yaml") final TestPluggableInterface testPluggableInterface) {
        assertThat(testPluggableInterface.getOptionAValue(), equalTo("some-value-for-a"));
    }

    @Test
    void parameter_of_EventFactory_gets_an_instance(final EventFactory eventFactory) {
        assertThat(eventFactory, notNullValue());
    }

    @Test
    void parameter_of_EventKeyFactory_gets_an_instance(final EventKeyFactory eventKeyFactory) {
        assertThat(eventKeyFactory, notNullValue());
    }
}
