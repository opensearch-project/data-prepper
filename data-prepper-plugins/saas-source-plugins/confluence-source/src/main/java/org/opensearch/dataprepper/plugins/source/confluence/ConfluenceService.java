/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence;

import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.confluence.exception.BadRequestException;
import org.opensearch.dataprepper.plugins.source.confluence.models.IssueBean;
import org.opensearch.dataprepper.plugins.source.confluence.models.SearchResults;
import org.opensearch.dataprepper.plugins.source.confluence.rest.ConfluenceRestClient;
import org.opensearch.dataprepper.plugins.source.confluence.utils.ConfluenceConfigHelper;
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

import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.UPDATED;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.JqlConstants.CLOSING_ROUND_BRACKET;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.JqlConstants.DELIMITER;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.JqlConstants.GREATER_THAN_EQUALS;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.JqlConstants.ISSUE_TYPE_IN;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.JqlConstants.ISSUE_TYPE_NOT_IN;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.JqlConstants.PREFIX;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.JqlConstants.PROJECT_IN;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.JqlConstants.PROJECT_NOT_IN;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.JqlConstants.STATUS_IN;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.JqlConstants.STATUS_NOT_IN;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.JqlConstants.SUFFIX;


/**
 * Service class for interactive external Atlassian jira SaaS service and fetch required details using their rest apis.
 */

@Slf4j
@Named
public class ConfluenceService {


    public static final String CONTENT_TYPE = "ContentType";
    private static final String SEARCH_RESULTS_FOUND = "searchResultsFound";

    private final ConfluenceSourceConfig confluenceSourceConfig;
    private final ConfluenceRestClient confluenceRestClient;
    private final Counter searchResultsFoundCounter;
    private final PluginMetrics jiraPluginMetrics = PluginMetrics.fromNames("jiraService", "aws");


    public ConfluenceService(ConfluenceSourceConfig confluenceSourceConfig, ConfluenceRestClient confluenceRestClient) {
        this.confluenceSourceConfig = confluenceSourceConfig;
        this.confluenceRestClient = confluenceRestClient;
        this.searchResultsFoundCounter = jiraPluginMetrics.counter(SEARCH_RESULTS_FOUND);
    }

    /**
     * Get jira entities.
     *
     * @param configuration the configuration.
     * @param timestamp     timestamp.
     */
    public void getJiraEntities(ConfluenceSourceConfig configuration, Instant timestamp, Queue<ItemInfo> itemInfoQueue) {
        log.trace("Started to fetch entities");
        searchForNewTicketsAndAddToQueue(configuration, timestamp, itemInfoQueue);
        log.trace("Creating item information and adding in queue");
    }

    public String getIssue(String issueKey) {
        return confluenceRestClient.getIssue(issueKey);
    }

    /**
     * Method for building Issue Item Info.
     *
     * @param configuration Input Parameter
     * @param timestamp     Input Parameter
     */
    private void searchForNewTicketsAndAddToQueue(ConfluenceSourceConfig configuration, Instant timestamp,
                                                  Queue<ItemInfo> itemInfoQueue) {
        log.trace("Looking for Add/Modified tickets with a Search API call");
        StringBuilder jql = createIssueFilterCriteria(configuration, timestamp);
        int total;
        int startAt = 0;
        do {
            SearchResults searchIssues = confluenceRestClient.getAllIssues(jql, startAt, configuration);
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
            itemInfoQueue.add(ConfluenceItemInfo.builder().withEventTime(Instant.now()).withIssueBean(issue).build());
        });
    }


    /**
     * Method for creating Issue Filter Criteria.
     *
     * @param configuration Input Parameter
     * @param ts            Input Parameter
     * @return String Builder
     */
    private StringBuilder createIssueFilterCriteria(ConfluenceSourceConfig configuration, Instant ts) {

        log.info("Creating issue filter criteria");
        if (!CollectionUtils.isEmpty(ConfluenceConfigHelper.getProjectNameIncludeFilter(configuration)) || !CollectionUtils.isEmpty(ConfluenceConfigHelper.getProjectNameExcludeFilter(configuration))) {
            validateProjectFilters(configuration);
        }
        StringBuilder jiraQl = new StringBuilder(UPDATED + GREATER_THAN_EQUALS + ts.toEpochMilli());
        if (!CollectionUtils.isEmpty(ConfluenceConfigHelper.getProjectNameIncludeFilter(configuration))) {
            jiraQl.append(PROJECT_IN).append(ConfluenceConfigHelper.getProjectNameIncludeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(ConfluenceConfigHelper.getProjectNameExcludeFilter(configuration))) {
            jiraQl.append(PROJECT_NOT_IN).append(ConfluenceConfigHelper.getProjectNameExcludeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(ConfluenceConfigHelper.getIssueTypeIncludeFilter(configuration))) {
            jiraQl.append(ISSUE_TYPE_IN).append(ConfluenceConfigHelper.getIssueTypeIncludeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(ConfluenceConfigHelper.getIssueTypeExcludeFilter(configuration))) {
            jiraQl.append(ISSUE_TYPE_NOT_IN).append(ConfluenceConfigHelper.getIssueTypeExcludeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(ConfluenceConfigHelper.getIssueStatusIncludeFilter(configuration))) {
            jiraQl.append(STATUS_IN).append(ConfluenceConfigHelper.getIssueStatusIncludeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(ConfluenceConfigHelper.getIssueStatusExcludeFilter(configuration))) {
            jiraQl.append(STATUS_NOT_IN).append(ConfluenceConfigHelper.getIssueStatusExcludeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        log.error("Created issue filter criteria JiraQl query: {}", jiraQl);
        return jiraQl;
    }

    /**
     * Method for Validating Project Filters.
     *
     * @param configuration Input Parameter
     */
    private void validateProjectFilters(ConfluenceSourceConfig configuration) {
        log.trace("Validating project filters");
        List<String> badFilters = new ArrayList<>();
        Set<String> includedProjects = new HashSet<>();
        List<String> includedAndExcludedProjects = new ArrayList<>();
        Pattern regex = Pattern.compile("[^A-Z0-9]");
        ConfluenceConfigHelper.getProjectNameIncludeFilter(configuration).forEach(projectFilter -> {
            Matcher matcher = regex.matcher(projectFilter);
            includedProjects.add(projectFilter);
            if (matcher.find() || projectFilter.length() <= 1 || projectFilter.length() > 10) {
                badFilters.add(projectFilter);
            }
        });
        ConfluenceConfigHelper.getProjectNameExcludeFilter(configuration).forEach(projectFilter -> {
            Matcher matcher = regex.matcher(projectFilter);
            if (includedProjects.contains(projectFilter)) {
                includedAndExcludedProjects.add(projectFilter);
            }
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
        if (!includedAndExcludedProjects.isEmpty()) {
            String filters = String.join("\"" + includedAndExcludedProjects + "\"", ", ");
            log.error("One or more project keys found in both include and exclude: {}", includedAndExcludedProjects);
            throw new BadRequestException("Bad request exception occurred " +
                    "Project filters is invalid because the following projects are listed in both include and exclude"
                    + filters);
        }

    }

}