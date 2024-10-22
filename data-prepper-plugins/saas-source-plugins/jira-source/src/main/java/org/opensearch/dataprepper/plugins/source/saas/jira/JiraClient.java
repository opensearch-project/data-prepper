package org.opensearch.dataprepper.plugins.source.saas.jira;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.saas_crawler.base.SaasClient;
import org.opensearch.dataprepper.plugins.source.saas_crawler.base.SaasSourceConfig;
import org.opensearch.dataprepper.plugins.source.saas_crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.saas_crawler.model.ItemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.Iterator;

/**
 * This class represents a Jira client.
 */
@Named
public class JiraClient implements SaasClient {

    private static final Logger log = LoggerFactory.getLogger(JiraClient.class);
    private long lastPollTime;

    public JiraClient() {
    }


    @Override
    public Iterator<ItemInfo> listItems() {
        return null;
    }

    @Override
    public void setLastPollTime(long lastPollTime) {
        log.info("Setting the lastPollTime: {}", lastPollTime);
        this.lastPollTime = lastPollTime;
    }

    @Override
    public void executePartition(SaasWorkerProgressState state, Buffer<Record<Event>> buffer, SaasSourceConfig configuration) {
        log.info("Logic for executing the partitions");
    }
}
