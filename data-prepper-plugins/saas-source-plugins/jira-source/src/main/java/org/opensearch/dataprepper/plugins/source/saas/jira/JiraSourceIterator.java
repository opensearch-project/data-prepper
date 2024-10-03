package org.opensearch.dataprepper.plugins.source.saas.jira;

import org.opensearch.dataprepper.plugins.source.saas.crawler.base.ItemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Component
public class JiraSourceIterator implements Iterator<ItemInfo> {

    private static final Logger log = LoggerFactory.getLogger(JiraSourceIterator.class);
    private Queue<ItemInfo> itemInfoQueue;
    private final JiraSourceConfig sourceConfig;
    private final JiraService service;
    private long jiraChangeLogToken;
    private boolean firstTime = true;
    private List<Future<Boolean>> futureList = new ArrayList<>();
    private ExecutorService crawlerTaskExecutor = Executors.newFixedThreadPool(10);
    public static final int HAS_NEXT_TIMEOUT = 7200;

    public JiraSourceIterator(final JiraService service, final JiraSourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
        this.service = service;
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
                log.info("Waiting for crawling queue to be filled for next {} seconds.", timeout);
                Thread.sleep(1000);
                timeout--;
            } catch (InterruptedException e) {
                log.error("An exception has occurred while checking for next document in crawling queue.");
                Thread.currentThread().interrupt();
            }
        }

        if (!isCrawlerRunning()
                && !crawlerTaskExecutor.isTerminated()) {
            terminateExecutor();
        }
        return !this.itemInfoQueue.isEmpty();
    }

    private boolean isCrawlerRunning() {
        boolean isRunning = Boolean.FALSE;
        if (Objects.nonNull(futureList)) {
            for (Future<Boolean> future : futureList) {
                if (!future.isDone()) {
                    isRunning = Boolean.TRUE;
                    break;
                }
            }
        }
        return isRunning;
    }

    private void terminateExecutor() {
        try {
            log.debug("Shutting down ExecutorService " + crawlerTaskExecutor);
            crawlerTaskExecutor.shutdown();
            boolean isExecutorTerminated = crawlerTaskExecutor
                    .awaitTermination(30, TimeUnit.SECONDS);
            log.debug("ExecutorService terminated : " + isExecutorTerminated);
        } catch (InterruptedException e) {
            log.error("Interrupted while terminating executor : " + e.getMessage());
            Thread.currentThread().interrupt();
        } finally {
            crawlerTaskExecutor.shutdownNow();
        }
    }

    private void startCrawlerThreads() {
        futureList.add(crawlerTaskExecutor.submit(
                () -> service.getJiraEntities(JiraConfiguration.of(sourceConfig), jiraChangeLogToken, itemInfoQueue,
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
    public void initialize(long jiraChangeLogToken) {
        this.itemInfoQueue = new ConcurrentLinkedQueue<>();
        this.jiraChangeLogToken = jiraChangeLogToken;
    }
}
