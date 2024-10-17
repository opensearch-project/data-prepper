package org.opensearch.dataprepper.plugins.source.saas.crawler.base;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
public class CrawlerTest {
    @Mock
    private SaasSourceConfig sourceConfig;

    @Mock
    private long lastPollTime;

    @Mock
    private EnhancedSourceCoordinator coordinator;

    @Mock
    private SaasClient client;

    private Crawler crawler;


    @BeforeEach
    public void setup() {
        crawler = new Crawler(client);
    }

    @Test
    public void crawlerConstructionTest() {
        assertNotNull(crawler);
    }

    @Test
    public void crawlTest() {
//        crawler.crawl(sourceConfig, lastPollTime, coordinator);
    }

}
