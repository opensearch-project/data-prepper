package org.opensearch.dataprepper.acknowledgements;

import org.opensearch.dataprepper.model.acknowledgements.ExpiryItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ExpiryMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(ExpiryMonitor.class);

    private static final long CANCEL_CHECK_INTERVAL_SECONDS = 15;
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();
    private static final ConcurrentHashMap<ExpiryItem, ScheduledFuture> EXPIRY_MONITORS = new ConcurrentHashMap<>();

    public ExpiryMonitor() {
        SCHEDULED_EXECUTOR_SERVICE.scheduleAtFixedRate(() -> {
            EXPIRY_MONITORS.forEach((expiryItem, future) -> {
                final boolean isCompleteOrExpired = expiryItem.isCompleteOrExpired();
                LOG.error("ExpiryItem with ID {} has completion/expiry status {}", expiryItem.getItemId(), isCompleteOrExpired);

                if (isCompleteOrExpired) {
                    future.cancel(false);
                    EXPIRY_MONITORS.remove(expiryItem);
                }
            });
        }, 0, CANCEL_CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    public void addExpiryItem(final ExpiryItem expiryItem) {
        final ScheduledFuture expiryMonitor = SCHEDULED_EXECUTOR_SERVICE.scheduleAtFixedRate(monitorExpiration(expiryItem),
                0, expiryItem.getPollSeconds() - 2, TimeUnit.SECONDS);
        EXPIRY_MONITORS.put(expiryItem, expiryMonitor);
    }

    private Runnable monitorExpiration(final ExpiryItem expiryItem) {
        return () -> {
            final boolean callBackSuccess = expiryItem.executeExpiryCallback();
            if (callBackSuccess) {
                expiryItem.updateExpirationTime();
            }
        };
    }
}
