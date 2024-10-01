package org.opensearch.dataprepper.plugins.source.saas.crawler.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Component
public class Crawler {
    private static final Logger log = LoggerFactory.getLogger(Crawler.class);
    private static final int maxItemsPerPage = 50;

    private final Iterator<ItemInfo> sourceIterator;
//    private final SaasSourceConfig sourceConfig;

    public Crawler(Iterator<ItemInfo> sourceIterator) {
        this.sourceIterator = sourceIterator;
//        this.sourceConfig = sourceConfig;
    }

    public void crawl(SaasSourceConfig sourceConfig) {
        log.info("Starting to crawl the source");
        final List<ItemInfo> itemInfoList = new ArrayList<>();
        for(int i = 0; i < maxItemsPerPage && sourceIterator.hasNext(); i++) {
            itemInfoList.add(sourceIterator.next());
        }

    }

}
