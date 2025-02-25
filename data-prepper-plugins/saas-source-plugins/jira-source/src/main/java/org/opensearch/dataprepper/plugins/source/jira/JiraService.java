/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira;

import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.source.jira.models.IssueBean;
import org.opensearch.dataprepper.plugins.source.jira.models.SearchResults;
import org.opensearch.dataprepper.plugins.source.jira.rest.JiraRestClient;
import org.opensearch.dataprepper.plugins.source.jira.utils.JiraConfigHelper;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.springframework.util.CollectionUtils;

import javax.inject.Named;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.UPDATED;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.CLOSING_ROUND_BRACKET;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.DELIMITER;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.GREATER_THAN_EQUALS;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.ISSUE_TYPE_IN;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.ISSUE_TYPE_NOT_IN;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.PREFIX;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.PROJECT_IN;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.PROJECT_NOT_IN;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.STATUS_IN;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.STATUS_NOT_IN;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.SUFFIX;


/**
 * Service class for interactive external Atlassian jira SaaS service and fetch required details using their rest apis.
 */

@Slf4j
@Named
public class JiraService {


    public static final String CONTENT_TYPE = "ContentType";
    private static final String SEARCH_RESULTS_FOUND = "searchResultsFound";

    private final JiraSourceConfig jiraSourceConfig;
    private final JiraRestClient jiraRestClient;
    private final Counter searchResultsFoundCounter;


    public JiraService(JiraSourceConfig jiraSourceConfig, JiraRestClient jiraRestClient, PluginMetrics pluginMetrics) {
        this.jiraSourceConfig = jiraSourceConfig;
        this.jiraRestClient = jiraRestClient;
        this.searchResultsFoundCounter = pluginMetrics.counter(SEARCH_RESULTS_FOUND);
    }

    /**
     * Get jira entities.
     *
     * @param configuration the configuration.
     * @param timestamp     timestamp.
     * @param itemInfoQueue item info queue
     */
    public void getJiraEntities(JiraSourceConfig configuration, Instant timestamp, Queue<ItemInfo> itemInfoQueue) {
        log.trace("Started to fetch entities");
        searchForNewTicketsAndAddToQueue(configuration, timestamp, itemInfoQueue);
        log.trace("Creating item information and adding in queue");
    }

    public String getIssue(String issueKey) {
        return jiraRestClient.getIssue(issueKey);
    }

    /**
     * Method for building Issue Item Info.
     *
     * @param configuration Input Parameter
     * @param timestamp     Input Parameter
     * @param itemInfoQueue item info queue
     */
    private void searchForNewTicketsAndAddToQueue(JiraSourceConfig configuration, Instant timestamp,
                                                  Queue<ItemInfo> itemInfoQueue) {
        log.trace("Looking for Add/Modified tickets with a Search API call");
        StringBuilder jql = createIssueFilterCriteria(configuration, timestamp);
        int total;
        int startAt = 0;
        do {
            SearchResults searchIssues = jiraRestClient.getAllIssues(jql, startAt);
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
        });
    }


    /**
     * Method for creating Issue Filter Criteria.
     *
     * @param configuration Input Parameter
     * @param ts            Input Parameter
     * @return String Builder created issue filter criteria
     */
    private StringBuilder createIssueFilterCriteria(JiraSourceConfig configuration, Instant ts) {

        log.info("Creating issue filter criteria");
        if (!CollectionUtils.isEmpty(JiraConfigHelper.getProjectNameIncludeFilter(configuration)) || !CollectionUtils.isEmpty(JiraConfigHelper.getProjectNameExcludeFilter(configuration))) {
            validateProjectFilters(configuration);
        }
        StringBuilder jiraQl = new StringBuilder(UPDATED + GREATER_THAN_EQUALS + ts.toEpochMilli());
        if (!CollectionUtils.isEmpty(JiraConfigHelper.getProjectNameIncludeFilter(configuration))) {
            jiraQl.append(PROJECT_IN).append(JiraConfigHelper.getProjectNameIncludeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(JiraConfigHelper.getProjectNameExcludeFilter(configuration))) {
            jiraQl.append(PROJECT_NOT_IN).append(JiraConfigHelper.getProjectNameExcludeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(JiraConfigHelper.getIssueTypeIncludeFilter(configuration))) {
            jiraQl.append(ISSUE_TYPE_IN).append(JiraConfigHelper.getIssueTypeIncludeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(JiraConfigHelper.getIssueTypeExcludeFilter(configuration))) {
            jiraQl.append(ISSUE_TYPE_NOT_IN).append(JiraConfigHelper.getIssueTypeExcludeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(JiraConfigHelper.getIssueStatusIncludeFilter(configuration))) {
            jiraQl.append(STATUS_IN).append(JiraConfigHelper.getIssueStatusIncludeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(JiraConfigHelper.getIssueStatusExcludeFilter(configuration))) {
            jiraQl.append(STATUS_NOT_IN).append(JiraConfigHelper.getIssueStatusExcludeFilter(configuration).stream()
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
        log.trace("Validating project filters");
        List<String> badFilters = new ArrayList<>();
        Set<String> includedProjects = new HashSet<>();
        List<String> includedAndExcludedProjects = new ArrayList<>();
        Pattern regex = Pattern.compile("[^A-Z0-9]");
        JiraConfigHelper.getProjectNameIncludeFilter(configuration).forEach(projectFilter -> {
            Matcher matcher = regex.matcher(projectFilter);
            includedProjects.add(projectFilter);
            if (matcher.find() || projectFilter.length() <= 1 || projectFilter.length() > 100) {
                badFilters.add(projectFilter);
            }
        });
        JiraConfigHelper.getProjectNameExcludeFilter(configuration).forEach(projectFilter -> {
            Matcher matcher = regex.matcher(projectFilter);
            if (includedProjects.contains(projectFilter)) {
                includedAndExcludedProjects.add(projectFilter);
            }
            if (matcher.find() || projectFilter.length() <= 1 || projectFilter.length() > 100) {
                badFilters.add(projectFilter);
            }
        });
        if (!badFilters.isEmpty()) {
            String filters = String.join("\"" + badFilters + "\"", ", ");
            log.error("One or more invalid project keys found in filter configuration: {}", badFilters);
            throw new InvalidPluginConfigurationException("Bad request exception occurred " +
                    "Invalid project key found in filter configuration for "
                    + filters);
        }
        if (!includedAndExcludedProjects.isEmpty()) {
            String filters = String.join("\"" + includedAndExcludedProjects + "\"", ", ");
            log.error("One or more project keys found in both include and exclude: {}", includedAndExcludedProjects);
            throw new InvalidPluginConfigurationException("Bad request exception occurred " +
                    "Project filters is invalid because the following projects are listed in both include and exclude"
                    + filters);
        }

    }

}
