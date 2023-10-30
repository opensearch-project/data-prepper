package org.opensearch.dataprepper.acknowledgements;

import com.google.common.annotations.VisibleForTesting;
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
    private final ScheduledExecutorService scheduledExecutorService;
    private final ConcurrentHashMap<ExpiryItem, ScheduledFuture> expiryMonitors;

    public ExpiryMonitor() {
        this(Executors.newSingleThreadScheduledExecutor());
    }

    @VisibleForTesting
    ExpiryMonitor(final ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.expiryMonitors = new ConcurrentHashMap<>();

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            expiryMonitors.forEach((expiryItem, future) -> {
                final boolean isCompleteOrExpired = expiryItem.isCompleteOrExpired();

                if (isCompleteOrExpired) {
                    LOG.info("ExpiryItem with ID {} has completed or expired", expiryItem.getItemId());
                    future.cancel(false);
                    expiryMonitors.remove(expiryItem);
                }
            });
        }, 0, CANCEL_CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    public void addExpiryItem(final ExpiryItem expiryItem) {
        final ScheduledFuture expiryMonitor = scheduledExecutorService.scheduleAtFixedRate(monitorExpiration(expiryItem),
                0, expiryItem.getPollSeconds() - 2, TimeUnit.SECONDS);
        LOG.info("{}", expiryItem);
        LOG.info("{}", expiryMonitor);
        LOG.info("{}", expiryMonitors);
        expiryMonitors.put(expiryItem, expiryMonitor);
    }

    private Runnable monitorExpiration(final ExpiryItem expiryItem) {
        return () -> {
            final boolean callBackSuccess = expiryItem.executeExpiryCallback();
            if (callBackSuccess) {
                expiryItem.updateExpirationTime();
            }
        };
    }

    public void shutdown() {
        scheduledExecutorService.shutdownNow();
    }

    @VisibleForTesting
    ConcurrentHashMap<ExpiryItem, ScheduledFuture> getExpiryMonitors() {
        return expiryMonitors;
    }
}
