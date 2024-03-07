package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class ConcurrentRequestSender implements RequestSender {
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentRequestSender.class);

    private final List<Future<Void>> pendingRequestFutures;
    private final CompletionService<Void> completionService;
    private final int concurrentRequestCount;
    private final ReentrantLock reentrantLock;

    public ConcurrentRequestSender(final int concurrentRequestCount) {
        this.concurrentRequestCount = concurrentRequestCount;
        pendingRequestFutures = new ArrayList<>();
        completionService = new ExecutorCompletionService(Executors.newFixedThreadPool(concurrentRequestCount));
        reentrantLock = new ReentrantLock();
    }

    @VisibleForTesting
    ConcurrentRequestSender(final int concurrentRequestCount, final CompletionService<Void> completionService) {
        this.concurrentRequestCount = concurrentRequestCount;
        pendingRequestFutures = new ArrayList<>();
        this.completionService = completionService;
        reentrantLock = new ReentrantLock();
    }

    @Override
    public void sendRequest(final Consumer<AccumulatingBulkRequest> requestConsumer, final AccumulatingBulkRequest request) {
        reentrantLock.lock();

        if (isRequestQueueFull()) {
            waitForRequestSlot();
        }

        final Future<Void> future = completionService.submit(convertConsumerIntoCallable(requestConsumer, request));
        pendingRequestFutures.add(future);

        reentrantLock.unlock();
    }

    private Callable<Void> convertConsumerIntoCallable(final Consumer<AccumulatingBulkRequest> requestConsumer, final AccumulatingBulkRequest request) {
        return () -> {
            requestConsumer.accept(request);
            return null;
        };
    }

    private void waitForRequestSlot() {
        do {
            checkFutureCompletion();
            if (isRequestQueueFull()) {
                try {
                    LOG.debug("Request queue is full, waiting for slot to free up");
                    completionService.take();
                } catch (final Exception e) {
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
                }
                iterator.remove();
            } else if (future.isDone()) {
                iterator.remove();
            }
        }
    }
}
