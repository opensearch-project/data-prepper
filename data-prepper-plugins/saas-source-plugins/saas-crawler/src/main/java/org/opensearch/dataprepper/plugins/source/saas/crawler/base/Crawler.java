package org.opensearch.dataprepper.plugins.source.saas.crawler.base;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.saas.crawler.model.ItemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class Crawler {
    private static final Logger log = LoggerFactory.getLogger(Crawler.class);
    private static final int maxItemsPerPage = 2;
    public static final String CREATED = "created";
    public static final String UPDATED = "updated";

    private final SaasClient client;

    public Crawler(SaasClient client) {
        this.client = client;
    }

    public long crawl(SaasSourceConfig sourceConfig,
                      long lastPollTime,
                      EnhancedSourceCoordinator coordinator) {
        long startTime = System.currentTimeMillis();
        client.setConfiguration(sourceConfig);
        client.setLastPollTime(lastPollTime);
        Iterator<ItemInfo> itemInfoIterator = client.listItems();
        log.info("Starting to crawl the source");
        long updatedPollTime = 0;
        do {
            final List<ItemInfo> itemInfoList = new ArrayList<>();
            for (int i = 0; i < maxItemsPerPage && itemInfoIterator.hasNext(); i++) {
                ItemInfo nextItem = itemInfoIterator.next();
                if(nextItem==null) {
                    //we don't expect null items, but just in case, we'll skip them
                    continue;
                }
                itemInfoList.add(nextItem);
                long niCreated = Long.parseLong(nextItem.getMetadata().get(CREATED)!=null?nextItem.getMetadata().get(CREATED):"0");
                long niUpdated = Long.parseLong(nextItem.getMetadata().get(UPDATED)!=null?nextItem.getMetadata().get(UPDATED):"0");
                updatedPollTime = Math.max(updatedPollTime, niCreated);
                updatedPollTime = Math.max(updatedPollTime, niUpdated);
            }
            createPartition(itemInfoList, coordinator);
        }while (itemInfoIterator.hasNext());
        log.info("Crawling completed in {} ms", System.currentTimeMillis() - startTime);
        return updatedPollTime!=0?updatedPollTime:startTime;
    }

    public void executePartition(SaasWorkerProgressState state, Buffer<Record<Event>> buffer) {
        client.executePartition(state, buffer);
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
        log.info("Creating a new partition");
        SaasSourcePartition sourcePartition = new SaasSourcePartition(state, partitionKey);
        coordinator.createPartition(sourcePartition);
    }

}
