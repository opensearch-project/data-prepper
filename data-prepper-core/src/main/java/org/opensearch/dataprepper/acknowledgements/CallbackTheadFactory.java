/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CallbackTheadFactory implements ThreadFactory {
    private final ThreadFactory delegateFactory;
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    public CallbackTheadFactory(final ThreadFactory delegateFactory) {
        this.delegateFactory = Objects.requireNonNull(delegateFactory);
    }

    @Override
    public Thread newThread(final Runnable runnable) {
        final Thread thread = delegateFactory.newThread(runnable);
        thread.setName("acknowledgement-callback-" + threadNumber.getAndIncrement());
        return thread;
    }
}
