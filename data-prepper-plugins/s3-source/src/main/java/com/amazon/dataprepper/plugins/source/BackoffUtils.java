/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class BackoffUtils {
    private static final Logger LOG = LoggerFactory.getLogger(BackoffUtils.class);

    private int numberOfRetries;
    private long timeToWait;

    private final Random random = new Random();

    public BackoffUtils(final int numberOfRetries, final long timeToWait) {
        this.numberOfRetries = numberOfRetries;
        this.timeToWait = timeToWait;
    }

    public boolean shouldRetry() {
        return numberOfRetries > 0;
    }

    public void errorOccurred() {
        numberOfRetries -= 1;

        if (shouldRetry()) {
            waitUntilNextTry();
            timeToWait += random.nextInt(1000);
        }
    }

    private void waitUntilNextTry() {
        try {
            Thread.sleep(timeToWait);
        } catch (InterruptedException e) {
            LOG.error("Thread is interrupted.", e);
        }
    }

    public void doNotRetry() {
        numberOfRetries = 0;
    }

    public int getNumberOfTriesLeft() {
        return numberOfRetries;
    }
}
