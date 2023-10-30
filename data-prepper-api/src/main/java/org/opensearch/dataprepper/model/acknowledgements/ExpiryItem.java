package org.opensearch.dataprepper.model.acknowledgements;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;

public class ExpiryItem {
    private static final Logger LOG = LoggerFactory.getLogger(ExpiryItem.class);

    private final String itemId;
    private final Instant startTime;
    private final long pollSeconds;
    private final Consumer expiryCallback;
    private Instant expirationTime;
    private AcknowledgementSet acknowledgementSet;

    public ExpiryItem(final String itemId, final Instant startTime, final long pollSeconds, final Instant expirationTime,
                      final Consumer<ExpiryItem> expiryCallback, final AcknowledgementSet acknowledgementSet) {
        this.itemId = itemId;
        this.startTime = startTime;
        if (pollSeconds <= 2) {
            throw new UnsupportedOperationException("The poll interval must be at least 3 seconds to enable expiry monitoring");
        }
        this.pollSeconds = pollSeconds;
        this.expirationTime = expirationTime;
        this.expiryCallback = expiryCallback;
        this.acknowledgementSet = acknowledgementSet;
    }

    public long getPollSeconds() {
        return pollSeconds;
    }

    public String getItemId() {
        return itemId;
    }

    public boolean executeExpiryCallback() {
        try {
            expiryCallback.accept(this);
            return true;
        } catch (final Exception e) {
            LOG.error("Exception occurred when executing the expiry callback", e.getMessage());
            return false;
        }
    }

    public boolean isCompleteOrExpired() {
        return acknowledgementSet.isDone() || Instant.now().isAfter(expirationTime);
    }

    public void updateExpirationTime() {
        LOG.info("Updating expiration time for item ID {} to {}", itemId, expirationTime);
        expirationTime = expirationTime.plusSeconds(pollSeconds);
        acknowledgementSet.setExpiryTime(expirationTime);
    }

    public long getSecondsBetweenStartAndExpiration() {
        return Duration.between(startTime, expirationTime).getSeconds();
    }
}
