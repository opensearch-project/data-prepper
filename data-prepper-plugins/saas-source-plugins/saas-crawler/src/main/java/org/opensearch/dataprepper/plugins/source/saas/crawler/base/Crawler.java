package org.opensearch.dataprepper.plugins.source.saas.crawler.base;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
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

    private final Iterator<ItemInfo> sourceIterator;

    public Crawler(Iterator<ItemInfo> sourceIterator) {
        this.sourceIterator = sourceIterator;
    }

    public void crawl(SaasSourceConfig sourceConfig,
                      long lastPollTime,
                      EnhancedSourceCoordinator coordinator) {
        log.info("Starting to crawl the source");
        do {
            final List<ItemInfo> itemInfoList = new ArrayList<>();
            for (int i = 0; i < maxItemsPerPage && sourceIterator.hasNext(); i++) {
                itemInfoList.add(sourceIterator.next());
            }
            createPartition(itemInfoList, coordinator);
        }while (sourceIterator.hasNext());
    }

    private void createPartition(List<ItemInfo> itemInfoList, EnhancedSourceCoordinator coordinator) {
        List<String> itemIds = itemInfoList.stream().map(ItemInfo::getId).collect(Collectors.toList());
        SaasWorkerProgressState state = new SaasWorkerProgressState();
        state.setItemIds(itemIds);
        state.setExportStartTime(System.currentTimeMillis());
        state.setLoadedItems(itemInfoList.size());
        log.info("Creating a new partition");
        SaasSourcePartition sourcePartition = new SaasSourcePartition(state, "jira", "project1", "ISSUE");
        coordinator.createPartition(sourcePartition);
    }

}
