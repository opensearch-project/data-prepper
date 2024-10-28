package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SaasPluginExecutorServiceProviderTest {

    private SaasPluginExecutorServiceProvider provider;
    private ExecutorService executorService;

    private SaasPluginExecutorServiceProvider provider2;
    @Mock
    private ExecutorService mockExecutorService;

    @BeforeEach
    void setUp() {
        provider = new SaasPluginExecutorServiceProvider();
        executorService = provider.get();
    }

    @AfterEach
    void tearDown() {
        provider.terminateExecutor();
    }

    @Test
    void testConstruction() {
        assertNotNull(executorService);
        assertNotNull(provider);
    }

    @Test
    void testTerminateExecutor() {
        provider.terminateExecutor();
        assertTrue(executorService.isShutdown());
        assertTrue(executorService.isTerminated());
    }

    @Test
    void terminateExecutorInterruptionTest() throws InterruptedException {
        provider2 = new SaasPluginExecutorServiceProvider(mockExecutorService);
        when(mockExecutorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException());
        AtomicBoolean wasInterrupted = new AtomicBoolean(false);

        Thread testThread = new Thread(() -> {
            provider2.terminateExecutor();
            wasInterrupted.set(Thread.interrupted());
        });
        testThread.start();
        testThread.join();

        assertTrue(wasInterrupted.get());
    }
}