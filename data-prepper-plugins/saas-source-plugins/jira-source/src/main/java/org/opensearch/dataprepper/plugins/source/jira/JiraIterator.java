package org.opensearch.dataprepper.plugins.source.jira;


import lombok.Setter;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.time.Instant;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

@Named
public class JiraIterator implements Iterator<ItemInfo> {

    private static final Logger log = LoggerFactory.getLogger(JiraIterator.class);
    private final JiraSourceConfig sourceConfig;
    private final JiraService service;
    private final ExecutorService crawlerTaskExecutor;
    @Setter
    private long crawlerQWaitTimeMillis = 2000;
    private Queue<ItemInfo> itemInfoQueue;
    private Instant lastPollTime;
    private boolean firstTime = true;

    public JiraIterator(final JiraService service,
                        PluginExecutorServiceProvider executorServiceProvider,
                        JiraSourceConfig sourceConfig) {
        this.service = service;
        this.crawlerTaskExecutor = executorServiceProvider.get();
        this.sourceConfig = sourceConfig;
    }

    @Override
    public boolean hasNext() {
        if (firstTime) {
            log.trace("Crawling has been started");
            itemInfoQueue = service.getJiraEntities(sourceConfig, lastPollTime);
            firstTime = false;
        }
        return !this.itemInfoQueue.isEmpty();
    }

    @Override
    public ItemInfo next() {
        if (hasNext()) {
            return this.itemInfoQueue.remove();
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Initialize.
     *
     * @param jiraChangeLogToken the jira change log token
     */
    public void initialize(Instant jiraChangeLogToken) {
        this.itemInfoQueue = new ConcurrentLinkedQueue<>();
        this.lastPollTime = jiraChangeLogToken;
        this.firstTime = true;
    }

}
