package org.opensearch.dataprepper.plugins.source.saas.jira;

import lombok.Setter;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.SaasClient;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.SaasSourceConfig;
import org.opensearch.dataprepper.plugins.source.saas.crawler.model.Item;
import org.opensearch.dataprepper.plugins.source.saas.crawler.model.ItemInfo;
import org.opensearch.dataprepper.plugins.source.saas.jira.models.IssueBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.Iterator;
import java.util.Optional;

import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.ISSUE_KEY;

/**
 * This class represents a Jira client.
 */
@Named
public class JiraClient implements SaasClient {

    private static final Logger log = LoggerFactory.getLogger(JiraClient.class);

    private final JiraService service;
    private JiraConfiguration configuration;
    private final JiraIterator jiraIterator;
    private long lastPollTime;

    public JiraClient(JiraService service, JiraIterator jiraIterator) {
        this.service = service;
        this.jiraIterator = jiraIterator;
    }

    @Override
    public Optional<ItemInfo> getItemInfo(ItemInfo itemInfo) {
        IssueBean issue = service.getIssue(itemInfo.getMetadata().get(ISSUE_KEY), configuration);
        Optional<Item> resultItem = buildIssueItem(issue, itemInfo, configuration);
        return resultItem.map(item -> itemInfo);
    }

    private Optional<Item> buildIssueItem(IssueBean issue, ItemInfo itemInfo, JiraConfiguration configuration) {
        //TODO: Convert issue to Item
        return Optional.empty();
    }

    @Override
    public Iterator<ItemInfo> listItems() {
        jiraIterator.initialize(lastPollTime);
        return jiraIterator;
    }

    @Override
    public void setConfiguration(SaasSourceConfig configuration) {
        this.configuration = JiraConfiguration.of((JiraSourceConfig) configuration);
        this.jiraIterator.setSourceConfig(configuration);
    }

    @Override
    public void setLastPollTime(long lastPollTime) {
        this.lastPollTime = lastPollTime;
    }
}
