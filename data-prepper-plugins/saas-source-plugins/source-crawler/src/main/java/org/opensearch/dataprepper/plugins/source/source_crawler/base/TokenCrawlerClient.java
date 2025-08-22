package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.PaginationCrawlerWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;

import java.util.Iterator;

/**
 * Interface for Crawler client to support token-based pagination. This interface can 
 * be implemented by different saas clients.
 */
public interface TokenCrawlerClient<T extends SaasWorkerProgressState> {

    /**
     * This will be the main API called by crawler. This method assumes that {@link
     * CrawlerSourceConfig} is available as a member to {@link CrawlerClient}, as a result of
     * which, other scanning properties will also be available to this method
     *
     * @return returns an {@link Iterator} of {@link ItemInfo}
     */
    Iterator<ItemInfo> listItems(String lastToken);

    void executePartition(PaginationCrawlerWorkerProgressState state,
                          Buffer<Record<Event>> buffer,
                          AcknowledgementSet acknowledgementSet);
}

