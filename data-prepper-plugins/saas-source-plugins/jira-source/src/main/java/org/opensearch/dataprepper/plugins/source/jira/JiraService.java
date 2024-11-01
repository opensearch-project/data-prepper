package org.opensearch.dataprepper.plugins.source.jira;

import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.jira.exception.BadRequestException;
import org.opensearch.dataprepper.plugins.source.jira.models.IssueBean;
import org.opensearch.dataprepper.plugins.source.jira.models.SearchResults;
import org.opensearch.dataprepper.plugins.source.jira.rest.JiraRestClient;
import org.opensearch.dataprepper.plugins.source.jira.utils.JiraConfigHelper;
import org.opensearch.dataprepper.plugins.source.jira.utils.JiraContentType;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.springframework.util.CollectionUtils;

import javax.inject.Named;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.ISSUE_KEY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.KEY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.LIVE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PROJECT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PROJECT_KEY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.UPDATED;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants._PROJECT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.CLOSING_ROUND_BRACKET;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.DELIMITER;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.GREATER_THAN_EQUALS;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.ISSUE_TYPE_IN;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.PREFIX;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.PROJECT_IN;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.STATUS_IN;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.SUFFIX;


/**
 * Service class for interactive external Atlassian jira SaaS service and fetch required details using their rest apis.
 */

@Slf4j
@Named
public class JiraService {


    public static final String CONTENT_TYPE = "ContentType";
    private static final String SEARCH_RESULTS_FOUND = "searchResultsFound";
    private static final Map<String, String> jiraProjectCache = new ConcurrentHashMap<>();

    private final JiraSourceConfig jiraSourceConfig;
    private final JiraRestClient jiraRestClient;
    private final Counter searchResultsFoundCounter;
    private final PluginMetrics jiraPluginMetrics = PluginMetrics.fromNames("jiraService", "aws");


    public JiraService(JiraSourceConfig jiraSourceConfig, JiraRestClient jiraRestClient) {
        this.jiraSourceConfig = jiraSourceConfig;
        this.jiraRestClient = jiraRestClient;
        this.searchResultsFoundCounter = jiraPluginMetrics.counter(SEARCH_RESULTS_FOUND);
    }

    /**
     * Get jira entities.
     *
     * @param configuration the configuration.
     * @param timestamp     timestamp.
     * @param itemInfoQueue Item info queue.
     */
    public void getJiraEntities(JiraSourceConfig configuration, Instant timestamp,
                                Queue<ItemInfo> itemInfoQueue) {
        log.info("Started to fetch entities");
        jiraProjectCache.clear();
        searchForNewTicketsAndAddToQueue(configuration, timestamp, itemInfoQueue);
        log.trace("Creating item information and adding in queue");
        jiraProjectCache.keySet().forEach(key -> {
            Map<String, Object> metadata = new HashMap<>();
            metadata.put(CONTENT_TYPE, JiraContentType.PROJECT.getType());
            ItemInfo itemInfo = createItemInfo(_PROJECT + key, metadata);
            itemInfoQueue.add(itemInfo);
        });
    }

    public String getIssue(String issueKey) {
        return jiraRestClient.getIssue(issueKey);
    }

    /**
     * Method for building Issue Item Info.
     *
     * @param configuration Input Parameter
     * @param timestamp     Input Parameter
     */
    private void searchForNewTicketsAndAddToQueue(JiraSourceConfig configuration, Instant timestamp,
                                                  Queue<ItemInfo> itemInfoQueue) {
        log.trace("Looking for Add/Modified tickets with a Search API call");
        StringBuilder jql = createIssueFilterCriteria(configuration, timestamp);
        int total;
        int startAt = 0;
        do {
            SearchResults searchIssues = jiraRestClient.getAllIssues(jql, startAt, configuration);
            List<IssueBean> issueList = new ArrayList<>(searchIssues.getIssues());
            total = searchIssues.getTotal();
            startAt += searchIssues.getIssues().size();
            addItemsToQueue(issueList, itemInfoQueue);
        } while (startAt < total);
        searchResultsFoundCounter.increment(total);
        log.info("Number of tickets found in search api call: {}", total);
    }

    /**
     * Add items to queue.
     *
     * @param issueList     Issue list.
     * @param itemInfoQueue Item info queue.
     */
    private void addItemsToQueue(List<IssueBean> issueList, Queue<ItemInfo> itemInfoQueue) {
        issueList.forEach(issue -> {
            itemInfoQueue.add(JiraItemInfo.builder().withEventTime(Instant.now()).withIssueBean(issue).build());

            if (Objects.nonNull(((Map) issue.getFields().get(PROJECT)).get(KEY))) {
                String projectKey = ((Map) issue.getFields().get(PROJECT)).get(KEY).toString();
                if (!jiraProjectCache.containsKey(projectKey)) {
                    jiraProjectCache.put(projectKey, LIVE);
                }
            }
        });
    }


    /**
     * Method for creating Issue Filter Criteria.
     *
     * @param configuration Input Parameter
     * @param ts            Input Parameter
     * @return String Builder
     */
    private StringBuilder createIssueFilterCriteria(JiraSourceConfig configuration, Instant ts) {

        log.info("Creating issue filter criteria");
        if (!CollectionUtils.isEmpty(JiraConfigHelper.getProjectKeyFilter(configuration))) {
            validateProjectFilters(configuration);
        }
        StringBuilder jiraQl = new StringBuilder(UPDATED + GREATER_THAN_EQUALS + ts.toEpochMilli());
        if (!CollectionUtils.isEmpty(JiraConfigHelper.getProjectKeyFilter(configuration))) {
            jiraQl.append(PROJECT_IN).append(JiraConfigHelper.getProjectKeyFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(JiraConfigHelper.getIssueTypeFilter(configuration))) {
            jiraQl.append(ISSUE_TYPE_IN).append(JiraConfigHelper.getIssueTypeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(JiraConfigHelper.getIssueStatusFilter(configuration))) {
            jiraQl.append(STATUS_IN).append(JiraConfigHelper.getIssueStatusFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        log.info("Created issue filter criteria JiraQl query: {}", jiraQl);
        return jiraQl;
    }

    /**
     * Method for Validating Project Filters.
     *
     * @param configuration Input Parameter
     */
    private void validateProjectFilters(JiraSourceConfig configuration) {
        log.info("Validating project filters");
        List<String> badFilters = new ArrayList<>();
        Pattern regex = Pattern.compile("[^A-Z0-9]");
        JiraConfigHelper.getProjectKeyFilter(configuration).forEach(projectFilter -> {
            Matcher matcher = regex.matcher(projectFilter);
            if (matcher.find() || projectFilter.length() <= 1 || projectFilter.length() > 10) {
                badFilters.add(projectFilter);
            }
        });
        if (!badFilters.isEmpty()) {
            String filters = String.join("\"" + badFilters + "\"", ", ");
            log.error("One or more invalid project keys found in filter configuration: {}", badFilters);
            throw new BadRequestException("Bad request exception occurred " +
                    "Invalid project key found in filter configuration for "
                    + filters);
        }
    }

    /**
     * Method for creating Item Info.
     *
     * @param key      Input Parameter
     * @param metadata Input Parameter
     * @return Item Info
     */
    private ItemInfo createItemInfo(String key, Map<String, Object> metadata) {
        return JiraItemInfo.builder().withEventTime(Instant.now())
                .withId((String) metadata.get(ISSUE_KEY))
                .withItemId(key)
                .withMetadata(metadata)
                .withProject((String) metadata.get(PROJECT_KEY))
                .withIssueType((String) metadata.get(CONTENT_TYPE))
                .build();
    }

}