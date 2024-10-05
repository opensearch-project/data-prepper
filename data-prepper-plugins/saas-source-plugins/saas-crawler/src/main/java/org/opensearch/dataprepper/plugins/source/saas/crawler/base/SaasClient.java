package org.opensearch.dataprepper.plugins.source.saas.crawler.base;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.saas.crawler.model.Item;
import org.opensearch.dataprepper.plugins.source.saas.crawler.model.ItemInfo;

import java.util.Iterator;
import java.util.Optional;

/**
 * Interface for saas client. This interface can be implemented by different saas clients.
 * For example, Jira, Salesforce, ServiceNow, etc.
 */
public interface SaasClient {

    /**
     * Get item information by item identifier.
     *
     * @param itemInfo metadata info the item that we are interested in
     * @return item information
     */
    Optional<Item> getItem(ItemInfo itemInfo);

    /**
     * This will be the main API called by crawler. This method assumes that {@link
     * SaasSourceConfig} is available as a member to {@link SaasClient}, as a result of
     * which, other scanning properties will also be available to this method
     *
     * @return returns an {@link Iterator} of {@link ItemInfo}
     */
    Iterator<ItemInfo> listItems();

    /**
     * This API returns most recent change log token of a SaaS Client. It will be called by SDK to
     * store the token, so that it can be utilized in next sync call if change log is
     * enabled.
     *
     * @return optional of token String
     */
    default Optional<String> getLatestChangeLogToken() {
        return Optional.empty();
    }

    /**
     * Set configuration for saas client.
     *
     * @param configuration {@link SaasSourceConfig}
     */
     void setConfiguration(SaasSourceConfig configuration);

    void setLastPollTime(long lastPollTime);

    void executePartition(SaasWorkerProgressState state, Buffer<Record<Event>> buffer);
}
