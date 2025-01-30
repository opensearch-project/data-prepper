/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence;


import com.google.common.annotations.VisibleForTesting;
import lombok.Setter;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@Named
public class ConfluenceIterator implements Iterator<ItemInfo> {

    private static final int HAS_NEXT_TIMEOUT = 60;
    private static final Logger log = LoggerFactory.getLogger(ConfluenceIterator.class);
    @Setter
    private long crawlerQWaitTimeMillis = 2000;
    private Queue<ItemInfo> itemInfoQueue;
    private Instant lastPollTime;
    private boolean firstTime = true;
    private final List<Future<Boolean>> futureList = new ArrayList<>();
    private final ConfluenceSourceConfig sourceConfig;
    private final ConfluenceService service;
    private final ExecutorService crawlerTaskExecutor;

    public ConfluenceIterator(final ConfluenceService service,
                              PluginExecutorServiceProvider executorServiceProvider,
                              ConfluenceSourceConfig sourceConfig) {
        this.service = service;
        this.crawlerTaskExecutor = executorServiceProvider.get();
        this.sourceConfig = sourceConfig;
    }

    @Override
    public boolean hasNext() {
        if (firstTime) {
            log.trace("Crawling has been started");
            startCrawlerThreads();
            firstTime = false;
        }
        int timeout = HAS_NEXT_TIMEOUT;
        while (isCrawlerRunning() && itemInfoQueue.isEmpty() && timeout > 0) {
            try {
                log.trace("Waiting for crawler queue to be filled for next {} seconds", timeout);
                Thread.sleep(crawlerQWaitTimeMillis);
                timeout--;
            } catch (InterruptedException e) {
                log.error("An exception has occurred while checking for the next document in crawling queue");
                Thread.currentThread().interrupt();
            }
        }
        return !this.itemInfoQueue.isEmpty();
    }

    private boolean isCrawlerRunning() {
        boolean isRunning = false;
        if (!futureList.isEmpty()) {
            for (Future<Boolean> future : futureList) {
                if (!future.isDone()) {
                    isRunning = true;
                    break;
                }
            }
        }
        return isRunning;
    }

    private void startCrawlerThreads() {
        futureList.add(crawlerTaskExecutor.submit(() ->
                service.getJiraEntities(sourceConfig, lastPollTime, itemInfoQueue), false));
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

    @VisibleForTesting
    public List<Future<Boolean>> showFutureList() {
        return futureList;
    }

    @VisibleForTesting
    public Queue<ItemInfo> showItemInfoQueue() {
        return itemInfoQueue;
    }

}
