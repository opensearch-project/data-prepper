/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.service.Office365Service;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.springframework.util.CollectionUtils;

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

/**
 * Iterator implementation for Office 365 audit logs.
 * Manages the asynchronous fetching and iteration of audit log entries.
 */
@Slf4j
@Named
public class Office365Iterator implements Iterator<ItemInfo> {
    private static final int HAS_NEXT_TIMEOUT = 60;

    private final Office365Service service;
    private final ExecutorService crawlerTaskExecutor;

    @Setter
    private long crawlerQWaitTimeMillis = 2000;
    private Queue<ItemInfo> itemInfoQueue;
    private Instant lastPollTime;
    private boolean firstTime = true;
    private final List<Future<Boolean>> futureList;

    public Office365Iterator(final Office365Service service,
                             final PluginExecutorServiceProvider executorServiceProvider) {
        this.service = service;
        this.crawlerTaskExecutor = executorServiceProvider.get();
        this.futureList = new ArrayList<>();
    }

    @Override
    public boolean hasNext() {
        if (firstTime) {
            log.debug("Starting initial crawl for Office 365 audit logs");
            startCrawlerThreads();
            firstTime = false;
        }

        int timeout = HAS_NEXT_TIMEOUT;
        while (isCrawlerRunning() && isQueueEmpty() && timeout > 0) {
            try {
                log.trace("Waiting for crawler queue to be filled, timeout in {} seconds", timeout);
                Thread.sleep(crawlerQWaitTimeMillis);
                timeout--;
            } catch (InterruptedException e) {
                log.error("Thread interrupted while waiting for crawler queue", e);
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return !isQueueEmpty();
    }

    @Override
    public ItemInfo next() {
        if (hasNext()) {
            return itemInfoQueue.remove();
        }
        throw new NoSuchElementException("No more items available in the Office 365 audit log queue");
    }

    public void initialize(final Instant startTime) {
        log.info("Initializing Office 365 iterator from timestamp: {}", startTime);
        this.itemInfoQueue = new ConcurrentLinkedQueue<>();
        this.lastPollTime = startTime;
        this.firstTime = true;
        this.futureList.clear();
    }

    private boolean isCrawlerRunning() {
        if (CollectionUtils.isEmpty(futureList)) {
            return false;
        }
        return futureList.stream().anyMatch(future -> !future.isDone());
    }

    private boolean isQueueEmpty() {
        return itemInfoQueue == null || itemInfoQueue.isEmpty();
    }

    void startCrawlerThreads() {
        log.debug("Starting crawler thread for Office 365 audit logs");
        Future<Boolean> future = crawlerTaskExecutor.submit(() -> {
            try {
                service.getOffice365Entities(lastPollTime, itemInfoQueue);
                return true;
            } catch (Exception e) {
                log.error("Error in crawler thread while fetching Office 365 audit logs", e);
                return false;
            }
        });
        futureList.add(future);
    }
}
