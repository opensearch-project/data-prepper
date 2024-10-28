package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import javax.inject.Named;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Named
public class PluginExecutorServiceProvider {
    private static final Logger log = LoggerFactory.getLogger(PluginExecutorServiceProvider.class);
    private static final int DEFAULT_THREAD_COUNT = 50;
    private final ExecutorService executorService;

    public PluginExecutorServiceProvider() {
        executorService = Executors.newFixedThreadPool(DEFAULT_THREAD_COUNT);
    }

    /**
     * Constructor for testing
     */
    public PluginExecutorServiceProvider(ExecutorService testExecutorService) {
        executorService = testExecutorService;
    }

    public ExecutorService get() {
        return executorService;
    }

    @PreDestroy
    public void terminateExecutor() {
        try {
            log.debug("Shutting down ExecutorService " + executorService);
            executorService.shutdown();
            boolean isExecutorTerminated = executorService
                    .awaitTermination(30, TimeUnit.SECONDS);
            log.debug("ExecutorService terminated : " + isExecutorTerminated);
        } catch (InterruptedException e) {
            log.error("Interrupted while terminating executor : " + e.getMessage());
            Thread.currentThread().interrupt();
        } finally {
            executorService.shutdownNow();
        }
    }
}