package org.opensearch.dataprepper.plugins.source.saas.jira;

import org.opensearch.dataprepper.plugins.source.saas.crawler.base.ItemInfo;
import org.springframework.stereotype.Component;

import java.util.Iterator;

@Component
public class JiraSourceIterator implements Iterator<ItemInfo> {
    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public ItemInfo next() {
        return null;
    }
}
