package org.opensearch.dataprepper.plugins.source.saas_crawler.base;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.saas_crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.saas_crawler.model.ItemInfo;

import java.util.Iterator;

/**
 * Interface for saas client. This interface can be implemented by different saas clients.
 * For example, Jira, Salesforce, ServiceNow, etc.
 */
public interface SaasClient {


    /**
     * This will be the main API called by crawler. This method assumes that {@link
     * SaasSourceConfig} is available as a member to {@link SaasClient}, as a result of
     * which, other scanning properties will also be available to this method
     *
     * @return returns an {@link Iterator} of {@link ItemInfo}
     */
    Iterator<ItemInfo> listItems();


    /**
     * Method to set the last time we polled the service to check for any changes.
     *
     * @param lastPollTime time in milliseconds
     */
    void setLastPollTime(long lastPollTime);

    /**
     * Method for executing a particular partition or a chunk of work
     *
     * @param state worker node state holds the details of this particular chunk of work
     * @param buffer pipeline buffer to write the results into
     * @param sourceConfig pipeline configuration from the yaml
     */
    void executePartition(SaasWorkerProgressState state, Buffer<Record<Event>> buffer, SaasSourceConfig sourceConfig);
}
