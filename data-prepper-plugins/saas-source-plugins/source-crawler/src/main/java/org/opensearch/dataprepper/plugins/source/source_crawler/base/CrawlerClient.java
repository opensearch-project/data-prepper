package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;

import java.time.Instant;
import java.util.Iterator;

/**
 * Interface for Crawler client. This interface can be implemented by different saas clients.
 * For example, Jira, Salesforce, ServiceNow, etc.
 */
public interface CrawlerClient {


    /**
     * This will be the main API called by crawler. This method assumes that {@link
     * CrawlerSourceConfig} is available as a member to {@link CrawlerClient}, as a result of
     * which, other scanning properties will also be available to this method
     *
     * @return returns an {@link Iterator} of {@link ItemInfo}
     */
    Iterator<ItemInfo> listItems(Instant lastPollTime);
    

    /**
     * Method for executing a particular partition or a chunk of work
     *
     * @param state              worker node state holds the details of this particular chunk of work
     * @param buffer             pipeline buffer to write the results into
     * @param acknowledgementSet acknowledgement set to be used to track the completion of the partition
     */
    void executePartition(SaasWorkerProgressState state, Buffer<Record<Event>> buffer, AcknowledgementSet acknowledgementSet);
}
