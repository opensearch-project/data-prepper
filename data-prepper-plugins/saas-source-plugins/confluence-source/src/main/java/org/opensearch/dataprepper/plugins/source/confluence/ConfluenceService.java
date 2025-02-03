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
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluenceItem;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluenceSearchResults;
import org.opensearch.dataprepper.plugins.source.confluence.rest.ConfluenceRestClient;
import org.opensearch.dataprepper.plugins.source.confluence.utils.ConfluenceConfigHelper;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.springframework.util.CollectionUtils;

import javax.inject.Named;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.LAST_MODIFIED;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.CLOSING_ROUND_BRACKET;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.CONTENT_TYPE_IN;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.CONTENT_TYPE_NOT_IN;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.DELIMITER;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.GREATER_THAN_EQUALS;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.PREFIX;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.SPACE_IN;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.SPACE_NOT_IN;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.SUFFIX;


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
    public void getPages(ConfluenceSourceConfig configuration, Instant timestamp, Queue<ItemInfo> itemInfoQueue) {
        log.trace("Started to fetch entities");
        searchForNewContentAndAddToQueue(configuration, timestamp, itemInfoQueue);
        log.trace("Creating item information and adding in queue");
    }

    public String getContent(String contentId) {
        return confluenceRestClient.getContent(contentId);
    }

    /**
     * Method for building Issue Item Info.
     *
     * @param configuration Input Parameter
     * @param timestamp     Input Parameter
     */
    private void searchForNewContentAndAddToQueue(ConfluenceSourceConfig configuration, Instant timestamp,
                                                  Queue<ItemInfo> itemInfoQueue) {
        log.trace("Looking for Add/Modified tickets with a Search API call");
        StringBuilder cql = createContentFilterCriteria(configuration, timestamp);
        int total;
        int startAt = 0;
        do {
            ConfluenceSearchResults searchIssues = confluenceRestClient.getAllContent(cql, startAt);
            List<ConfluenceItem> issueList = new ArrayList<>(searchIssues.getResults());
            total = searchIssues.getSize();
            startAt += searchIssues.getResults().size();
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
    private void addItemsToQueue(List<ConfluenceItem> issueList, Queue<ItemInfo> itemInfoQueue) {
        issueList.forEach(issue -> itemInfoQueue.add(ConfluenceItemInfo.builder()
                .withEventTime(Instant.now()).withIssueBean(issue).build()));
    }


    /**
     * Method for creating Content Filter Criteria.
     *
     * @param configuration Input Parameter
     * @param ts            Input Parameter
     * @return String Builder
     */
    private StringBuilder createContentFilterCriteria(ConfluenceSourceConfig configuration, Instant ts) {

        log.info("Creating content filter criteria");
        if (!CollectionUtils.isEmpty(ConfluenceConfigHelper.getSpacesNameIncludeFilter(configuration)) || !CollectionUtils.isEmpty(ConfluenceConfigHelper.getSpacesNameExcludeFilter(configuration))) {
            validateSpaceFilters(configuration);
        }
        String formattedTimeStamp = LocalDateTime.ofInstant(ts, ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
        StringBuilder cQl = new StringBuilder(LAST_MODIFIED + GREATER_THAN_EQUALS + "\"" + formattedTimeStamp + "\"");
        if (!CollectionUtils.isEmpty(ConfluenceConfigHelper.getSpacesNameIncludeFilter(configuration))) {
            cQl.append(SPACE_IN).append(ConfluenceConfigHelper.getSpacesNameIncludeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(ConfluenceConfigHelper.getSpacesNameExcludeFilter(configuration))) {
            cQl.append(SPACE_NOT_IN).append(ConfluenceConfigHelper.getSpacesNameExcludeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(ConfluenceConfigHelper.getContentTypeIncludeFilter(configuration))) {
            cQl.append(CONTENT_TYPE_IN).append(ConfluenceConfigHelper.getContentTypeIncludeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(ConfluenceConfigHelper.getContentTypeExcludeFilter(configuration))) {
            cQl.append(CONTENT_TYPE_NOT_IN).append(ConfluenceConfigHelper.getContentTypeExcludeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }

        log.error("Created issue filter criteria JiraQl query: {}", cQl);
        return cQl;
    }

    /**
     * Method for Validating Space Filters.
     *
     * @param configuration Input Parameter
     */
    private void validateSpaceFilters(ConfluenceSourceConfig configuration) {
        log.trace("Validating project filters");
        List<String> badFilters = new ArrayList<>();
        Set<String> includedProjects = new HashSet<>();
        List<String> includedAndExcludedSpaces = new ArrayList<>();
        Pattern regex = Pattern.compile("[^A-Z0-9]");
        ConfluenceConfigHelper.getSpacesNameIncludeFilter(configuration).forEach(projectFilter -> {
            Matcher matcher = regex.matcher(projectFilter);
            includedProjects.add(projectFilter);
            if (matcher.find() || projectFilter.length() <= 1 || projectFilter.length() > 10) {
                badFilters.add(projectFilter);
            }
        });
        ConfluenceConfigHelper.getSpacesNameExcludeFilter(configuration).forEach(projectFilter -> {
            Matcher matcher = regex.matcher(projectFilter);
            if (includedProjects.contains(projectFilter)) {
                includedAndExcludedSpaces.add(projectFilter);
            }
            if (matcher.find() || projectFilter.length() <= 1 || projectFilter.length() > 10) {
                badFilters.add(projectFilter);
            }
        });
        if (!badFilters.isEmpty()) {
            String filters = String.join("\"" + badFilters + "\"", ", ");
            log.error("One or more invalid Space keys found in filter configuration: {}", badFilters);
            throw new BadRequestException("Bad request exception occurred " +
                    "Invalid Space key found in filter configuration for "
                    + filters);
        }
        if (!includedAndExcludedSpaces.isEmpty()) {
            String filters = String.join("\"" + includedAndExcludedSpaces + "\"", ", ");
            log.error("One or more Space keys found in both include and exclude: {}", includedAndExcludedSpaces);
            throw new BadRequestException("Bad request exception occurred " +
                    "Space filters is invalid because the following space are listed in both include and exclude"
                    + filters);
        }

    }

}