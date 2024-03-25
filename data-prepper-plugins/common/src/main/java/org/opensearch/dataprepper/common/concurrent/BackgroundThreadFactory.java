/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.concurrent;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link ThreadFactory} that names threads with a prefix and
 * sets as daemon threads so that they do not interrupt shutdown.
 * <p>
 * The thread name will be <i>namePrefix</i>-<i>threadNumber</i>.
 */
public class BackgroundThreadFactory implements ThreadFactory {
    private final String namePrefix;
    private final ThreadFactory delegateThreadFactory;
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    BackgroundThreadFactory(final String namePrefix, final ThreadFactory delegateThreadFactory) {
        this.namePrefix = Objects.requireNonNull(namePrefix);
        if(namePrefix.isEmpty()) {
            throw new IllegalArgumentException("The thread factory was given an empty namePrefix. It must be provided.");
        }
        this.delegateThreadFactory = Objects.requireNonNull(delegateThreadFactory);
    }

    /**
     * Creates a new instance of {@link BackgroundThreadFactory} with a specified namePrefix
     * using {@link Executors#defaultThreadFactory()} as the delegate {@link ThreadFactory}.
     *
     * @param namePrefix The prefix for the thread name.
     * @return A new instance.
     */
    public static BackgroundThreadFactory defaultExecutorThreadFactory(final String namePrefix) {
        return new BackgroundThreadFactory(namePrefix, Executors.defaultThreadFactory());
    }

    @Override
    public Thread newThread(final Runnable runnable) {
        final Thread thread = delegateThreadFactory.newThread(runnable);
        thread.setName(namePrefix + "-" + threadNumber.getAndIncrement());
        thread.setDaemon(false);

        return thread;
    }
}
