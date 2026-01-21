/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.plugin;

import org.junit.jupiter.api.Test;

public class ExtensionPluginTest {

    @Test
    void testShutdown() {
        final ExtensionPlugin extensionPlugin = new ExtensionPluginTestImpl();
        extensionPlugin.shutdown();
    }

    static class ExtensionPluginTestImpl implements ExtensionPlugin {

        @Override
        public void apply(ExtensionPoints extensionPoints) {

        }
    }
}
