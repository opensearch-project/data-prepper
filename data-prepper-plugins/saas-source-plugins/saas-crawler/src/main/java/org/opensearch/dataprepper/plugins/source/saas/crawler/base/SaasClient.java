package org.opensearch.dataprepper.plugins.source.saas.crawler.base;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.saas.crawler.model.ItemInfo;

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


    void setLastPollTime(long lastPollTime);

    void executePartition(SaasWorkerProgressState state, Buffer<Record<Event>> buffer, SaasSourceConfig sourceConfig);
}
