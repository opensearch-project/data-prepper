/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;

@DataPrepperPlugin(name="opensearch", pluginType = Source.class , pluginConfigurationType =OpenSearchSourceConfiguration.class )
public class OpenSearchSource implements Source<Record<Event>> {

    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @DataPrepperPluginConstructor
    public OpenSearchSource(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        startProcess(openSearchSourceConfiguration);
    }

    private void startProcess(final OpenSearchSourceConfiguration openSearchSourceConfiguration)  {
        // todo: implement
        // Should leverage OpenSearchService to run the actual plugin core logic.
    }

    @Override
    public void stop() {
      // Yet to implement
    }
}
