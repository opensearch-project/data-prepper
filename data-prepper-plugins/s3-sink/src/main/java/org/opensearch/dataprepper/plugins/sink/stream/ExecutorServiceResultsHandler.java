/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.stream;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wrapper around an ExecutorService that allows you to easily submit {@link Callable}s, get results via iteration,
 * and handle failure quickly. When a submitted callable throws an exception in its thread this
 * will result in a {@code RuntimeException} when iterating over results. Typical usage is as follows:
 * <p>
 * <ol>
 * <li>Create an ExecutorService and pass it to the constructor.</li>
 * <li>Create Callables and ensure that they respond to interruption, e.g. regularly call: <pre>{@code
 *     if (Thread.currentThread().isInterrupted()) {
 *         throw new RuntimeException("The thread was interrupted, likely indicating failure in a sibling thread.");
 *     }}</pre></li>
 * <li>Pass the callables to the {@code submit()} method.</li>
 * <li>Call {@code finishedSubmitting()}.</li>
 * <li>Iterate over this object (e.g. with a foreach loop) to get results from the callables.
 * Each iteration will block waiting for the next result.
 * If one of the callables throws an unhandled exception or the thread is interrupted during iteration
 * then {@link ExecutorService#shutdownNow()} will be called resulting in all still running callables being interrupted,
 * and a {@code RuntimeException} will be thrown </li>
 * </ol>
 * <p>
 * You can also call {@code abort()} to shut down the threads yourself.
 */
public class ExecutorServiceResultsHandler<V> implements Iterable<V> {

    private ExecutorCompletionService<V> completionService;
    private ExecutorService executorService;
    private AtomicInteger taskCount = new AtomicInteger(0);

    public ExecutorServiceResultsHandler(ExecutorService executorService) {
        this.executorService = executorService;
        completionService = new ExecutorCompletionService<V>(executorService);
    }

    public void submit(Callable<V> task) {
        completionService.submit(task);
        taskCount.incrementAndGet();
    }

    public void finishedSubmitting() {
        executorService.shutdown();
    }

    @Override
    public Iterator<V> iterator() {
        return new Iterator<V>() {
            @Override
            public boolean hasNext() {
                return taskCount.getAndDecrement() > 0;
            }

            @Override
            public V next() {
                Exception exception;
                try {
                    return completionService.take().get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    exception = e;
                } catch (ExecutionException e) {
                    exception = e;
                }
                abort();
                throw new RuntimeException(exception);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove");
            }

        };
    }

    public void abort() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    /**
     * Convenience method to wait for the callables to finish for when you don't care about the results.
     */
    public void awaitCompletion() {
        //noinspection StatementWithEmptyBody
        for (V ignored : this) {
            // do nothing
        }
    }

}
