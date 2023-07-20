/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.time;

import org.apache.commons.lang3.time.StopWatch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class SinkStopWatch {
    private final StopWatch stopWatch;
    private final ReentrantLock stopWatchLock;

    public SinkStopWatch() {
        stopWatch = StopWatch.create();
        stopWatchLock = new ReentrantLock();
    }

    public void startIfNotRunning() {
        stopWatchLock.lock();
        if (!stopWatch.isStarted()) {
            startStopWatch();
        }
        stopWatchLock.unlock();
    }

    public void startStopWatch() {
        stopWatchLock.lock();
        stopWatch.start();
        stopWatchLock.unlock();
    }

    public void stopAndResetStopWatch() {
        stopWatchLock.lock();
        stopWatch.stop();
        stopWatch.reset();
        stopWatch.start();
        stopWatchLock.unlock();
    }

    public long getStopWatchTimeSeconds() {
        stopWatchLock.lock();
        long time = stopWatch.getTime(TimeUnit.SECONDS);
        stopWatchLock.unlock();
        return time;
    }
}