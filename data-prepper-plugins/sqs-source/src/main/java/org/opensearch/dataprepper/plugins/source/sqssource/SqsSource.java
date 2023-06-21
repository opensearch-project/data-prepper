/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.source.sqssource.config.SqsSourceConfig;

@DataPrepperPlugin(name = "sqs", pluginType = Source.class,pluginConfigurationType = SqsSourceConfig.class)
public class SqsSource implements Source<Record<Event>> {

    private final SqsSourceConfig sqsSourceConfig;

    @DataPrepperPluginConstructor
    public SqsSource(final SqsSourceConfig sqsSourceConfig) {
        this.sqsSourceConfig = sqsSourceConfig;
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer is null");
        }
    }

    @Override
    public void stop() {
    }
}
