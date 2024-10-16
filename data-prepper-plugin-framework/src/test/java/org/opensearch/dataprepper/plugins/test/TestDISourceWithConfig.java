/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.test;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugin.TestPluggableInterface;
import org.opensearch.dataprepper.plugin.TestPluginConfiguration;

@DataPrepperPlugin(name = "test_di_source_with_config",
        alternateNames = { "test_source_alternate_name1", "test_source_alternate_name2" },
        deprecatedName = "test_source_deprecated_name",
        pluginType = Source.class,
        packagesToScan = {TestDISourceWithConfig.class},
        pluginConfigurationType = TestPluginConfiguration.class
)
public class TestDISourceWithConfig implements Source<Record<String>>, TestPluggableInterface {

    private final TestComponentWithConfigInject testComponent;

    @DataPrepperPluginConstructor
    public TestDISourceWithConfig(final TestComponentWithConfigInject testComponent) {
        this.testComponent = testComponent;
    }

    @Override
    public void start(Buffer<Record<String>> buffer) {
    }

    public TestComponentWithConfigInject getTestComponent() {
        return testComponent;
    }

    @Override
    public void stop() {}
}
