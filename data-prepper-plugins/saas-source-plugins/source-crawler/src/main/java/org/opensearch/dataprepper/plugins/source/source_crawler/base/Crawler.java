package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;

import java.time.Instant;

public interface Crawler<T extends SaasWorkerProgressState> {

    Instant crawl(LeaderPartition leaderPartition,
                  EnhancedSourceCoordinator coordinator);

    void executePartition(T state,
                          Buffer<Record<Event>> buffer,
                          AcknowledgementSet acknowledgementSet);

}
