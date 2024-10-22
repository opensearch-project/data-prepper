package org.opensearch.dataprepper.plugins.source.saas_crawler.base;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.saas_crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.saas_crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.saas_crawler.model.ItemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Named
public class Crawler {
    private static final Logger log = LoggerFactory.getLogger(Crawler.class);
    private static final int maxItemsPerPage = 20;
    public static final String CREATED = "created";
    public static final String UPDATED = "updated";

    private final SaasClient client;

    public Crawler(SaasClient client) {
        this.client = client;
    }

    public long crawl(long lastPollTime,
                      EnhancedSourceCoordinator coordinator) {
        long startTime = System.currentTimeMillis();
        client.setLastPollTime(lastPollTime);
        Iterator<ItemInfo> itemInfoIterator = client.listItems();
        log.info("Starting to crawl the source");
        long updatedPollTime = 0;
        log.info("Creating Partitions");
        do {
            final List<ItemInfo> itemInfoList = new ArrayList<>();
            for (int i = 0; i < maxItemsPerPage && itemInfoIterator.hasNext(); i++) {
                ItemInfo nextItem = itemInfoIterator.next();
                if(nextItem==null) {
                    //we don't expect null items, but just in case, we'll skip them
                    continue;
                }
                itemInfoList.add(nextItem);
                Map<String, String> metadata = nextItem.getMetadata();
                long niCreated = Long.parseLong(metadata.get(CREATED)!=null? metadata.get(CREATED):"0");
                long niUpdated = Long.parseLong(metadata.get(UPDATED)!=null? metadata.get(UPDATED):"0");
                updatedPollTime = Math.max(updatedPollTime, niCreated);
                updatedPollTime = Math.max(updatedPollTime, niUpdated);
                log.info("updated poll time {}", updatedPollTime);
            }
            createPartition(itemInfoList, coordinator);
        }while (itemInfoIterator.hasNext());
        log.info("Crawling completed in {} ms", System.currentTimeMillis() - startTime);
        updatedPollTime = updatedPollTime != 0 ? updatedPollTime + 1 : startTime;
        log.info("Updating last_poll_time to {}", updatedPollTime);
        return updatedPollTime;
    }

    public void executePartition(SaasWorkerProgressState state, Buffer<Record<Event>> buffer, SaasSourceConfig sourceConfig) {
        client.executePartition(state, buffer, sourceConfig);
    }

    private void createPartition(List<ItemInfo> itemInfoList, EnhancedSourceCoordinator coordinator) {
        if(itemInfoList.isEmpty()) {
            return;
        }
        ItemInfo itemInfo = itemInfoList.get(0);
        String partitionKey = itemInfo.getPartitionKey();
        List<String> itemIds = itemInfoList.stream().map(ItemInfo::getId).collect(Collectors.toList());
        SaasWorkerProgressState state = new SaasWorkerProgressState();
        state.setKeyAttributes(itemInfo.getKeyAttributes());
        state.setItemIds(itemIds);
        state.setExportStartTime(System.currentTimeMillis());
        state.setLoadedItems(itemInfoList.size());
        SaasSourcePartition sourcePartition = new SaasSourcePartition(state, partitionKey);
        coordinator.createPartition(sourcePartition);
    }

}
