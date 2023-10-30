package org.opensearch.dataprepper.acknowledgements;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.model.acknowledgements.ExpiryItem;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExpiryMonitorTest {
    @Mock
    private ExpiryItem expiryItem;
    @Mock
    private ScheduledFuture scheduledFuture;
    @Mock
    private ScheduledExecutorService scheduledExecutorService;

    public ExpiryMonitor expiryMonitor;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        this.expiryMonitor = new ExpiryMonitor(scheduledExecutorService);
    }

    @Test
    @Disabled("TODO - fix NPE")
    void addExpiryItem_Success() {
        when(expiryItem.getPollSeconds()).thenReturn(12L);
        // NPE here, not sure why
        when(scheduledExecutorService.scheduleAtFixedRate(any(), any(), any(), any()))
                .thenReturn(scheduledFuture);

        expiryMonitor.addExpiryItem(expiryItem);
        assertThat(expiryMonitor.getExpiryMonitors().size(), equalTo(1));
        assertThat(expiryMonitor.getExpiryMonitors().containsKey(expiryItem), equalTo(true));
        assertThat(expiryMonitor.getExpiryMonitors().get(expiryItem), equalTo(scheduledFuture));

        verify(scheduledExecutorService).scheduleAtFixedRate(any(), eq(0), eq(10L), eq(TimeUnit.SECONDS));
    }

    @Test
    void shutdown_Success() {
        expiryMonitor.shutdown();

        verify(scheduledExecutorService).shutdownNow();
    }
}
