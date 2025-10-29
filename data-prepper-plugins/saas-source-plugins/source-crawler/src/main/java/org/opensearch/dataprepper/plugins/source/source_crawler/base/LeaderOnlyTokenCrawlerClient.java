package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.PaginationCrawlerWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;

import java.util.List;

/**
 * Interface for leader-only token-based crawler client that extends TokenCrawlerClient.
 * This interface adds additional method for direct buffer writing,
 * optimized for single-leader processing without worker partitions.
 */
public interface LeaderOnlyTokenCrawlerClient extends TokenCrawlerClient<PaginationCrawlerWorkerProgressState> {
    /**
     * Writes a batch of items directly to the buffer.
     *
     * @param items The batch of items to write
     * @param buffer The buffer to write events to
     * @param acknowledgementSet Optional acknowledgment set for tracking write completion.
     *                          If provided, items will be added to this set for acknowledgment tracking.
     *                          Can be null if acknowledgments are disabled.
     */
    void writeBatchToBuffer(List<ItemInfo> items, Buffer<Record<Event>> buffer, AcknowledgementSet acknowledgementSet);
}