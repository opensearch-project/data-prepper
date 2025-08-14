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
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.dataprepper.plugin.PluginProvider;

import java.util.stream.Stream;

/**
 * Provides a common base class that plugin authors can use to create
 * Data Prepper plugin tests along with standard tests.
 *
 * @since 2.13
 */
@ExtendWith(DataPrepperPluginTestFrameworkExtension.class)
public class BaseDataPrepperPluginStandardTestSuite {
    @TestFactory
    Stream<DynamicTest> standardDataPrepperPluginTests(
            final DataPrepperPluginTestContext dataPrepperPluginTestContext,
            final PluginProvider pluginProvider) {
        return DataPrepperPluginAutoTestSuite.generateTests(dataPrepperPluginTestContext, pluginProvider);
    }
}
