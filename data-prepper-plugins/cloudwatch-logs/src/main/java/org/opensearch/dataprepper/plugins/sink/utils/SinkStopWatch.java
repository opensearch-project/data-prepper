package org.opensearch.dataprepper.plugins.sink.utils;

import org.apache.commons.lang3.time.StopWatch;

import java.util.concurrent.TimeUnit;

public class SinkStopWatch {
    private final StopWatch stopWatch;
    private boolean stopWatchOn;

    public SinkStopWatch() {
        stopWatch = StopWatch.create();
        stopWatchOn = false;
    }

    public void startIfNotRunning() {
        if (!stopWatchOn) {
            startStopWatch();
        }
    }

    public void startStopWatch() {
        stopWatchOn = true;
        stopWatch.start();
    }

    public void stopAndResetStopWatch() {
        stopWatchOn = false;
        stopWatch.stop();
        stopWatch.reset();
    }

    public long getStopWatchTimeSeconds() {
        return stopWatch.getTime(TimeUnit.SECONDS);
    }

    public boolean isStopWatchOn() {
        return stopWatchOn;
    }
}
