package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.TestItemInfo;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@ExtendWith(MockitoExtension.class)
public class CrawlerTest {
    @Mock
    private CrawlerSourceConfig sourceConfig;

    @Mock
    private EnhancedSourceCoordinator coordinator;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private CrawlerClient client;

    @Mock
    private SaasWorkerProgressState state;

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
    public void executePartitionTest() {
        crawler.executePartition(state, buffer, sourceConfig);
        verify(client).executePartition(state, buffer, sourceConfig);
    }

    @Test
    void testCrawlWithEmptyList() {
        Instant lastPollTime = Instant.ofEpochMilli(0);
        when(client.listItems()).thenReturn(Collections.emptyIterator());
        crawler.crawl(lastPollTime, coordinator);
        verify(coordinator, never()).createPartition(any(SaasSourcePartition.class));
    }

    @Test
    void testCrawlWithNonEmptyList() throws NoSuchFieldException, IllegalAccessException {
        Instant lastPollTime = Instant.ofEpochMilli(0);
        List<ItemInfo> itemInfoList = new ArrayList<>();
        int maxItemsPerPage = getMaxItemsPerPage();
        for (int i = 0; i < maxItemsPerPage; i++) {
            itemInfoList.add(new TestItemInfo("itemId"));
        }
        when(client.listItems()).thenReturn(itemInfoList.iterator());
        crawler.crawl(lastPollTime, coordinator);
        verify(coordinator, times(1)).createPartition(any(SaasSourcePartition.class));

    }

    @Test
    void testCrawlWithMultiplePartitions() throws NoSuchFieldException, IllegalAccessException {
        Instant lastPollTime = Instant.ofEpochMilli(0);
        List<ItemInfo> itemInfoList = new ArrayList<>();
        int maxItemsPerPage = getMaxItemsPerPage();
        for (int i = 0; i < maxItemsPerPage + 1; i++) {
            itemInfoList.add(new TestItemInfo("testId"));
        }
        when(client.listItems()).thenReturn(itemInfoList.iterator());
        crawler.crawl(lastPollTime, coordinator);
        verify(coordinator, times(2)).createPartition(any(SaasSourcePartition.class));

    }

    @Test
    void testCrawlWithNullItemsInList() throws NoSuchFieldException, IllegalAccessException {
        Instant lastPollTime = Instant.ofEpochMilli(0);
        List<ItemInfo> itemInfoList = new ArrayList<>();
        int maxItemsPerPage = getMaxItemsPerPage();
        itemInfoList.add(null);
        for (int i = 0; i < maxItemsPerPage - 1; i++) {
            itemInfoList.add(new TestItemInfo("testId"));
        }
        when(client.listItems()).thenReturn(itemInfoList.iterator());
        crawler.crawl(lastPollTime, coordinator);
        verify(coordinator, times(1)).createPartition(any(SaasSourcePartition.class));
    }

    @Test
    void testUpdatingPollTimeNullMetaData() {
        Instant lastPollTime = Instant.ofEpochMilli(0);
        List<ItemInfo> itemInfoList = new ArrayList<>();
        ItemInfo testItem = createTestItemInfo("1");
        itemInfoList.add(testItem);
        when(client.listItems()).thenReturn(itemInfoList.iterator());
        Instant updatedPollTime = crawler.crawl(lastPollTime, coordinator);
        assertNotEquals(Instant.ofEpochMilli(0), updatedPollTime);
    }

    @Test
    void testUpdatedPollTimeNiCreatedLarger() {
        Instant lastPollTime = Instant.ofEpochMilli(0);
        List<ItemInfo> itemInfoList = new ArrayList<>();
        ItemInfo testItem = createTestItemInfo("1");
        itemInfoList.add(testItem);
        when(client.listItems()).thenReturn(itemInfoList.iterator());
        Instant updatedPollTime = crawler.crawl(lastPollTime, coordinator);
        assertNotEquals(lastPollTime, updatedPollTime);
    }


    private int getMaxItemsPerPage() throws NoSuchFieldException, IllegalAccessException {
        Field maxItemsPerPageField = Crawler.class.getDeclaredField("maxItemsPerPage");
        maxItemsPerPageField.setAccessible(true);
        return (int) maxItemsPerPageField.get(null);
    }

    private ItemInfo createTestItemInfo(String id) {
        return new TestItemInfo(id, new HashMap<>(), Instant.now());
    }

}