package org.opensearch.dataprepper.plugins.sink.threshold;

import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;

/**
 * ThresholdCheck receives paramaters for which to reference the
 * limits of a buffer and CloudWatchLogsClient before making a
 * PutLogEvent request to AWS.
 */
public class ThresholdCheck {
    private final int batchSize;
    private final int maxEventSize;
    private final int maxRequestSize;
    private final int logSendInterval;

    ThresholdCheck (int batchSize, int maxEventSize, int maxRequestSize, int logSendInterval) {
        this.batchSize = batchSize;
        this.maxEventSize = maxEventSize;
        this.maxRequestSize = maxRequestSize;
        this.logSendInterval = logSendInterval;
    }

    public boolean checkLogSendInterval(int currentTime) {
        return currentTime >= logSendInterval;
    }

    public boolean checkMaxEventSize(int eventSize) {
        return eventSize > maxEventSize;
    }

    public boolean checkMaxRequestSize(int currentRequestSize) {
        return currentRequestSize >= maxRequestSize;
    }

    public boolean checkBatchSize(int batchSize) {
        return batchSize == this.batchSize;
    }
}
