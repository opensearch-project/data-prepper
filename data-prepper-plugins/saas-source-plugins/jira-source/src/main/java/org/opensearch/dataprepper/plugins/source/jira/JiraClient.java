package org.opensearch.dataprepper.plugins.source.jira;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerClient;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.time.Instant;
import java.util.Iterator;

/**
 * This class represents a Jira client.
 */
@Named
public class JiraClient implements CrawlerClient {

    private static final Logger log = LoggerFactory.getLogger(JiraClient.class);
    private Instant lastPollTime;

    public JiraClient() {
    }


    @Override
    public Iterator<ItemInfo> listItems() {
        return null;
    }

    @Override
    public void setLastPollTime(Instant lastPollTime) {
        log.info("Setting the lastPollTime: {}", lastPollTime);
        this.lastPollTime = lastPollTime;
    }

    @Override
    public void executePartition(SaasWorkerProgressState state, Buffer<Record<Event>> buffer, CrawlerSourceConfig configuration) {
        log.info("Logic for executing the partitions");
    }
}
