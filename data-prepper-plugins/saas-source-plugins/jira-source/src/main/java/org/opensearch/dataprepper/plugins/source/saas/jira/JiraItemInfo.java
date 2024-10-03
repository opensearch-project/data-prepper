package org.opensearch.dataprepper.plugins.source.saas.jira;

import org.opensearch.dataprepper.plugins.source.saas.crawler.base.ItemInfo;

public class JiraItemInfo extends ItemInfo {
    @Override
    public String getKeyAttributes() {
        return "project|issue-type";
    }
}
