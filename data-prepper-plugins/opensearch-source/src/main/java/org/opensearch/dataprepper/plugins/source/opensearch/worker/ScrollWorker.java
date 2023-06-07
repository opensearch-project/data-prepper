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
 * ScrollWorker polls the source cluster via scroll contexts.
 */
public class ScrollWorker implements SearchWorker {

    private final SearchAccessor searchAccessor;
    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;
    private final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;
    private final Buffer<Record<Event>> buffer;
    private final OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier;

    public ScrollWorker(final SearchAccessor searchAccessor,
                        final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                        final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator,
                        final Buffer<Record<Event>> buffer,
                        final OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier) {
        this.searchAccessor = searchAccessor;
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.sourceCoordinator = sourceCoordinator;
        this.buffer = buffer;
        this.openSearchIndexPartitionCreationSupplier = openSearchIndexPartitionCreationSupplier;
    }

    @Override
    public void run() {
        // todo: implement
    }
}
