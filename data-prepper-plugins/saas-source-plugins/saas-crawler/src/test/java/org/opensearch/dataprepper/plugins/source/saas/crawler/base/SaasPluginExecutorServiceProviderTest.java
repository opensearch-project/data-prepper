package org.opensearch.dataprepper.plugins.source.saas.crawler.base;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class SaasPluginExecutorServiceProviderTest {

    private SaasPluginExecutorServiceProvider provider;
    private ExecutorService executorService;

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
}
