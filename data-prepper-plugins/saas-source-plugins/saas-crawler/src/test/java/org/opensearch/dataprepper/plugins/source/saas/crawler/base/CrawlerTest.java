package org.opensearch.dataprepper.plugins.source.saas.crawler.base;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.saas.crawler.model.ItemInfo;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@ExtendWith(MockitoExtension.class)
public class CrawlerTest {
    @Mock
    private SaasSourceConfig sourceConfig;

    @Mock
    private EnhancedSourceCoordinator coordinator;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private SaasClient client;

    @Mock
    private SaasWorkerProgressState state;

    @Mock
    private ItemInfo item;

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
    void testCrawlWithEmptyList(){
        long lastPollTime = 0;
        when(client.listItems()).thenReturn(Collections.emptyIterator());
        crawler.crawl(lastPollTime, coordinator);
        verify(coordinator, never()).createPartition(any(SaasSourcePartition.class));
    }

    @Test
    void testCrawlWithNonEmptyList() throws NoSuchFieldException, IllegalAccessException {
        long lastPollTime = 0;
        List<ItemInfo> itemInfoList = new ArrayList<>();
        int maxItemsPerPage = getMaxItemsPerPage();
        for (int i = 0; i < maxItemsPerPage; i++) {
            itemInfoList.add(item);
        }
        when(client.listItems()).thenReturn(itemInfoList.iterator());
        crawler.crawl(lastPollTime, coordinator);
        verify(coordinator, times(1)).createPartition(any(SaasSourcePartition.class));

    }

    @Test
    void testCrawlWithMultiplePartitions()  throws NoSuchFieldException, IllegalAccessException{
        long lastPollTime = 0;
        List<ItemInfo> itemInfoList = new ArrayList<>();
        int maxItemsPerPage = getMaxItemsPerPage();
        for (int i = 0; i < maxItemsPerPage + 1; i++) {
            itemInfoList.add(item);
        }
        when(client.listItems()).thenReturn(itemInfoList.iterator());
        crawler.crawl(lastPollTime, coordinator);
        verify(coordinator, times(2)).createPartition(any(SaasSourcePartition.class));

    }

    @Test
    void testCrawlWithNullItemsInList() throws NoSuchFieldException, IllegalAccessException {
        long lastPollTime = 0;
        List<ItemInfo> itemInfoList = new ArrayList<>();
        int maxItemsPerPage = getMaxItemsPerPage();
        itemInfoList.add(null);
        for (int i = 0; i < maxItemsPerPage-1; i++) {
            itemInfoList.add(item);
        }
        when(client.listItems()).thenReturn(itemInfoList.iterator());
        crawler.crawl(lastPollTime, coordinator);
        verify(coordinator, times(1)).createPartition(any(SaasSourcePartition.class));
    }

    @Test
    void testUpdatingPollTimeNullMetaData() {
        long lastPollTime = 0;
        List<ItemInfo> itemInfoList = new ArrayList<>();
        ItemInfo testItem = createTestItemInfo("1",null, null);
        itemInfoList.add(testItem);
        when(client.listItems()).thenReturn(itemInfoList.iterator());
        long updatedPollTime = crawler.crawl(lastPollTime, coordinator);
        assert(updatedPollTime != 0);
    }

    @Test
    void testUpdatedPollTimeNiCreatedLarger() {
        long lastPollTime = 0;
        List<ItemInfo> itemInfoList = new ArrayList<>();
        ItemInfo testItem = createTestItemInfo("1","10" , "5");
        itemInfoList.add(testItem);
        when(client.listItems()).thenReturn(itemInfoList.iterator());
        long updatedPollTime = crawler.crawl(lastPollTime, coordinator);
        assert(updatedPollTime == 11);
    }
    @Test
    void testUpdatedPollTimeNiUpdatedLarger() {
        long lastPollTime = 0;
        List<ItemInfo> itemInfoList = new ArrayList<>();
        ItemInfo testItem = createTestItemInfo("1","5" , "10");
        itemInfoList.add(testItem);
        when(client.listItems()).thenReturn(itemInfoList.iterator());
        long updatedPollTime = crawler.crawl(lastPollTime, coordinator);
        assert(updatedPollTime == 11);
    }


    private int getMaxItemsPerPage() throws NoSuchFieldException, IllegalAccessException {
        Field maxItemsPerPageField = Crawler.class.getDeclaredField("maxItemsPerPage");
        maxItemsPerPageField.setAccessible(true);
        return (int) maxItemsPerPageField.get(null);
    }

    private static class TestItemInfo extends ItemInfo {
        public TestItemInfo(String itemId, Map<String, String> metadata, Long eventTime) {
            super(itemId, metadata, eventTime);

        }
        @Override
        public String getPartitionKey() {
            return getItemId();
        }

        @Override
        public String getId() {
            return getItemId();
        }

        @Override
        public Map<String, String> getKeyAttributes() {
            return new HashMap<>();
        }
    }

    private ItemInfo createTestItemInfo(String id, String created, String updated) {
        Map<String, String> metadata = new HashMap<>();
        if (created != null) metadata.put(Crawler.CREATED, created);
        if (updated != null) metadata.put(Crawler.UPDATED, updated);

        return new TestItemInfo(id, metadata, System.currentTimeMillis());
    }

}