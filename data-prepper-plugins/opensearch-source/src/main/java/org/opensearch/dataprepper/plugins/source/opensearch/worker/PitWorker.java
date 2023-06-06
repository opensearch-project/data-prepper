/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchIndexProgressState;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessor;

/**
 * PitWorker polls the source cluster via Point-In-Time contexts.
 */
public class PitWorker implements SearchWorker, Runnable {

    private final SearchAccessor searchAccessor;
    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;
    private final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;
    private final Buffer<Record<Event>> buffer;

    public PitWorker(final SearchAccessor searchAccessor,
                     final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                     final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator,
                     final Buffer<Record<Event>> buffer) {
        this.searchAccessor = searchAccessor;
        this.sourceCoordinator = sourceCoordinator;
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.buffer = buffer;
    }

    @Override
    public void run() {
        // todo: implement
    }
}
