package org.opensearch.dataprepper.plugins.source.jira;


import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@Named
public class JiraIterator implements Iterator<ItemInfo> {

    private static final int HAS_NEXT_TIMEOUT = 1000;
    private static final Logger log = LoggerFactory.getLogger(JiraIterator.class);
    private final JiraSourceConfig sourceConfig;
    private final JiraService service;
    private final ExecutorService crawlerTaskExecutor;
    private final List<Future<Boolean>> futureList = new ArrayList<>();
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
            log.info("Crawling has been started");
            startCrawlerThreads();
            firstTime = Boolean.FALSE;
        }
        int timeout = HAS_NEXT_TIMEOUT;
        while (isCrawlerRunning()
                && itemInfoQueue.isEmpty()
                && (timeout != 0)) {
            try {
                log.info("Waiting for crawling queue to be filled for next 2 seconds.");
                Thread.sleep(2000);
                timeout--;
            } catch (InterruptedException e) {
                log.error("An exception has occurred while checking for next document in crawling queue.");
                Thread.currentThread().interrupt();
            }
        }

        return !this.itemInfoQueue.isEmpty();
    }

    private boolean isCrawlerRunning() {
        boolean isRunning = Boolean.FALSE;
        if (!futureList.isEmpty()) {
            for (Future<Boolean> future : futureList) {
                if (!future.isDone()) {
                    isRunning = Boolean.TRUE;
                    break;
                }
            }
        }
        return isRunning;
    }


    private void startCrawlerThreads() {
        futureList.add(crawlerTaskExecutor.submit(
                () -> service.getJiraEntities(sourceConfig, lastPollTime, itemInfoQueue,
                        futureList, crawlerTaskExecutor), false));
    }

    @Override
    public ItemInfo next() {
        return this.itemInfoQueue.remove();
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
