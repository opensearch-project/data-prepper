package org.opensearch.dataprepper.model.acknowledgements;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.function.Consumer;

public class ExpiryItem {
    private static final Logger LOG = LoggerFactory.getLogger(ExpiryItem.class);

    private final String itemId;
    private final long pollSeconds;
    private final Consumer<ExpiryItem> expiryCallback;
    private Instant expirationTime;
    private AcknowledgementSet acknowledgementSet;

    public ExpiryItem(final String itemId, final long pollSeconds, final Instant expirationTime,
                      final Consumer<ExpiryItem> expiryCallback, final AcknowledgementSet acknowledgementSet) {
        this.itemId = itemId;
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

    public void setExpirationTime(final Instant expirationTime) {
        this.expirationTime = expirationTime;
    }

    public boolean executeExpiryCallback() {
        try {
            expiryCallback.accept(this);
            return true;
        } catch (final Exception e) {
            LOG.error("Exception occurred when executing the expiry callback: {}", e.getMessage());
            return false;
        }
    }

    public boolean isCompleteOrExpired() {
        return Instant.now().isAfter(expirationTime) || acknowledgementSet.isDone();
    }

    public void updateExpirationTime() {
        expirationTime = expirationTime.plusSeconds(pollSeconds);
        acknowledgementSet.setExpiryTime(expirationTime);
    }
}
