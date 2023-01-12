package org.opensearch.dataprepper.plugins.source.rss;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.document.Document;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class RSSSourceTest {

    private final String PLUGIN_NAME = "rss";

    private final String PIPELINE_NAME = "test";

    private final String VALID_RSS_URL = "https://forum.opensearch.org/latest.rss";

    @Mock
    private Buffer<Record<Document>> buffer;

    @Mock
    private RSSSourceConfig rssSourceConfig;

    private PluginMetrics pluginMetrics;

    private RSSSource rssSource;

    @BeforeEach
    void setUp() {
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, PIPELINE_NAME);
        lenient().when(rssSourceConfig.getUrl()).thenReturn(VALID_RSS_URL);
        lenient().when(rssSourceConfig.getPollingFrequency()).thenReturn(Duration.ofSeconds(5));
        rssSource = new RSSSource(pluginMetrics, rssSourceConfig);
    }

    @AfterEach
    public void tearDown() {
        rssSource.stop();
    }

    @Test
    void test_ExecutorService_keep_writing_Events_to_Buffer() throws InterruptedException, TimeoutException {
        rssSource.start(buffer);
        Thread.sleep(5000);
        verify(buffer, atLeastOnce()).write(any(Record.class), anyInt());
        Thread.sleep(5000);
        verify(buffer, atLeastOnce()).write(any(Record.class), anyInt());
    }
}
