package org.opensearch.dataprepper;

import com.google.rpc.RetryInfo;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

class GrpcRetryInfoCalculator {

    private final Duration minimumDelay;
    private final Duration maximumDelay;

    private final AtomicReference<Instant> lastTimeCalled;
    private final AtomicReference<Duration> nextDelay;

    GrpcRetryInfoCalculator(Duration minimumDelay, Duration maximumDelay) {
        this.minimumDelay = minimumDelay;
        this.maximumDelay = maximumDelay;
        // Create a cushion so that the calculator treats a first quick exception (after prepper startup) as normal request (e.g. does not calculate a backoff)
        this.lastTimeCalled = new AtomicReference<>(Instant.now().minus(maximumDelay));
        this.nextDelay = new AtomicReference<>(minimumDelay);
    }

    private static RetryInfo createProtoResult(Duration delay) {
        return RetryInfo.newBuilder().setRetryDelay(mapDuration(delay)).build();
    }

    private static Duration minDuration(Duration left, Duration right) {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static com.google.protobuf.Duration.Builder mapDuration(Duration duration) {
        return com.google.protobuf.Duration.newBuilder().setSeconds(duration.getSeconds()).setNanos(duration.getNano());
    }

    RetryInfo createRetryInfo() {
        Instant now = Instant.now();
        // Is the last time we got called longer ago than the next delay?
        if (lastTimeCalled.getAndSet(now).isBefore(now.minus(nextDelay.get()))) {
            // Use minimum delay and reset the saved delay
            nextDelay.set(minimumDelay);
            return createProtoResult(minimumDelay);
        }
        Duration delay = nextDelay.getAndUpdate(d -> minDuration(maximumDelay, d.multipliedBy(2)));
        return createProtoResult(delay);
    }

}
