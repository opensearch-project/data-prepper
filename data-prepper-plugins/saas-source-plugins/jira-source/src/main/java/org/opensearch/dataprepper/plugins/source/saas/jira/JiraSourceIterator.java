package org.opensearch.dataprepper.plugins.source.saas.jira;

import org.opensearch.dataprepper.plugins.source.saas.crawler.base.ItemInfo;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.UUID;

@Component
public class JiraSourceIterator implements Iterator<ItemInfo> {

    int itemCounter = 0;

    @Override
    public boolean hasNext() {
        return itemCounter<10;
    }

    @Override
    public ItemInfo next() {
        ItemInfo itemInfo = new ItemInfo();
        itemInfo.setId(UUID.randomUUID().toString());
        itemCounter++;
        return itemInfo;
    }
}
