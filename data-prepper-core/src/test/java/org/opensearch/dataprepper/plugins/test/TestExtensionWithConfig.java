/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.test;

import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;
import org.opensearch.dataprepper.model.plugin.ExtensionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@DataPrepperExtensionPlugin(modelType = TestExtensionConfig.class, rootKeyJsonPath = "test_extension")
public class TestExtensionWithConfig implements ExtensionPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(TestExtensionWithConfig.class);
    private static final AtomicInteger CONSTRUCTED_COUNT = new AtomicInteger(0);
    private final String extensionId;
    private TestExtensionConfig testExtensionConfig;

    @DataPrepperPluginConstructor
    public TestExtensionWithConfig(final TestExtensionConfig testExtensionConfig) {
        LOG.info("Constructing test extension plugin.");
        CONSTRUCTED_COUNT.incrementAndGet();
        extensionId = UUID.randomUUID().toString();
        this.testExtensionConfig = testExtensionConfig;
    }

    @Override
    public void apply(final ExtensionPoints extensionPoints) {
        LOG.info("Applying test extension.");
        extensionPoints.addExtensionProvider(new TestExtensionProvider());
    }

    public static void reset() {
        CONSTRUCTED_COUNT.set(0);
    }

    public static int getConstructedInstances() {
        return CONSTRUCTED_COUNT.get();
    }

    public static class TestModel {
        private final String extensionId;

        private final String testAttribute;

        private TestModel(final String extensionId, final String testAttribute) {

            this.extensionId = extensionId;
            this.testAttribute = testAttribute;
        }
        public String getExtensionId() {
            return this.extensionId;
        }

        public String getTestAttribute() {
            return testAttribute;
        }
    }

    private class TestExtensionProvider implements ExtensionProvider<TestModel> {

        @Override
        public Optional<TestModel> provideInstance(final Context context) {
            return Optional.of(new TestModel(extensionId, testExtensionConfig.getTestAttribute()));
        }

        @Override
        public Class<TestModel> supportedClass() {
            return TestModel.class;
        }
    }
}
