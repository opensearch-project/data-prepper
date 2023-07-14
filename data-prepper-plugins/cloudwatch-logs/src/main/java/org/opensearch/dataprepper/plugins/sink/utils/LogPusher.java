package org.opensearch.dataprepper.plugins.sink.utils;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.plugins.sink.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;

import java.util.ArrayList;

public class LogPusher {
    private final Counter logEventSuccessCounter;
    private final Counter logEventFailCounter;
    private final Counter requestSuccessCount;
    private final Counter requestFailCount;
    final int retryCount;
    final long backOffTimeBase;

    static final Logger LOG = LoggerFactory.getLogger(LogPusher.class);
    public LogPusher(Counter logEventSuccessCounter, Counter logEventFailCounter, Counter requestSuccessCount, Counter requestFailCount, final int retryCount, final long backOffTimeBase) {
        this.logEventSuccessCounter = logEventSuccessCounter;
        this.logEventFailCounter = logEventFailCounter;
        this.requestSuccessCount = requestSuccessCount;
        this.requestFailCount = requestFailCount;
        this.retryCount = retryCount;
        this.backOffTimeBase = backOffTimeBase;
    }

    public boolean pushLogs(final Buffer buffer, final CloudWatchLogsClient cloudWatchLogsClient, final String logGroup, final String logStream) throws InterruptedException {
        boolean failedPost = true;
        int failCounter = 0;

        ArrayList<InputLogEvent> logEventList = new ArrayList<>();

        for (byte[] data: buffer.getBufferedData()) {
            InputLogEvent tempLogEvent = InputLogEvent.builder()
                    .message(new String(data))
                    .timestamp(System.currentTimeMillis())
                    .build();
            logEventList.add(tempLogEvent);
        }

        PutLogEventsRequest putLogEventsRequest = PutLogEventsRequest.builder()
                .logEvents(logEventList)
                .logGroupName(logGroup)
                .logStreamName(logStream)
                .build();

        while (failedPost && (failCounter < retryCount)) {
            try {
                cloudWatchLogsClient.putLogEvents(putLogEventsRequest);

                requestSuccessCount.increment();
                failedPost = false;

                //TODO: When a log is rejected by the service, we cannot send it, can probably push to a DLQ here.

            } catch (CloudWatchLogsException | SdkClientException e) {
                LOG.error("Failed to push logs with error: {}", e.getMessage());

                requestFailCount.increment();

                Thread.sleep(calculateBackOffTime(backOffTimeBase, failCounter));

                LOG.warn("Trying to retransmit request... {Attempt: " + (++failCounter) + "}");
            }
        }

        buffer.clearBuffer();

        if (failedPost) {
            logEventFailCounter.increment(logEventList.size());
            LOG.error("Error, timed out trying to push logs!");
        } else {
            logEventSuccessCounter.increment(logEventList.size());
            return true;
        }

        return false;
    }

    private long calculateBackOffTime(final long backOffTimeBase, final int failCounter) {
        return failCounter * backOffTimeBase;
    }
}
