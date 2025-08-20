/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.plugins.junit;

import org.junit.jupiter.api.DynamicTest;
import org.opensearch.dataprepper.plugin.PluginProvider;

import java.util.Optional;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;

class DataPrepperPluginAutoTestSuite {
    static Stream<DynamicTest> generateTests(
            final DataPrepperPluginTestContext dataPrepperPluginTestContext,
            final PluginProvider pluginProvider) {
        return Stream.of(
                DynamicTest.dynamicTest("Plugin can be found by the plugin framework", () ->
                        {
                            final Optional<Class<?>> optional = pluginProvider.findPluginClass(
                                    (Class) dataPrepperPluginTestContext.getPluginType(),
                                    dataPrepperPluginTestContext.getPluginName());

                            assertThat("Unable to locate the plugin class", optional.isPresent(), equalTo(true));
                        }

                ),
                DynamicTest.dynamicTest("Plugin name conforms to standard naming convention", () ->
                        assertThat(dataPrepperPluginTestContext.getPluginName(), matchesPattern("^[a-z0-9_]+$"))
                )
        );
    }
}
