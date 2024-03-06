package org.opensearch.dataprepper.plugins.sink.opensearch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

public class RequestSender {
    private static final Logger LOG = LoggerFactory.getLogger(RequestSender.class);

    private final List<Future<Void>> pendingRequestFutures;
    private final ExecutorService requestExecutor;
    private final CompletionService<Void> completionService;
    private final int concurrentRequestCount;
    private final ReentrantLock reentrantLock;

    public RequestSender(final int concurrentRequestCount) {
        this.concurrentRequestCount = concurrentRequestCount;
        pendingRequestFutures = new ArrayList<>();
        requestExecutor = Executors.newFixedThreadPool(concurrentRequestCount);
        completionService = new ExecutorCompletionService(requestExecutor);
        reentrantLock = new ReentrantLock();
    }

    public void sendRequest(final Callable<Void> requestRunnable) {
        reentrantLock.lock();

        if (pendingRequestFutures.size() >= concurrentRequestCount) {
            waitForRequestSlot();
        }

        final Future<Void> future = completionService.submit(requestRunnable);
        pendingRequestFutures.add(future);

        reentrantLock.unlock();
    }

    private void waitForRequestSlot() {
        do {
            checkFutureCompletion();
            if (isRequestQueueFull()) {
                try {
                    LOG.info("Request queue is full, waiting for slot to free up");
                    completionService.take();
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while waiting for future completion");
                }
            }
        } while (isRequestQueueFull());
    }

    private boolean isRequestQueueFull() {
        return pendingRequestFutures.size() >= concurrentRequestCount;
    }

    private void checkFutureCompletion() {
        for (Iterator<Future<Void>> iterator = pendingRequestFutures.iterator(); iterator.hasNext();) {
            final Future<Void> future = iterator.next();
            if (future.isCancelled()) {
                try {
                    future.get();
                } catch (final Exception e) {
                    LOG.error("Indexing future was cancelled", e);
                    iterator.remove();
                    return;
                }
            }

            if (future.isDone()) {
                iterator.remove();
            }
        }
    }
}
