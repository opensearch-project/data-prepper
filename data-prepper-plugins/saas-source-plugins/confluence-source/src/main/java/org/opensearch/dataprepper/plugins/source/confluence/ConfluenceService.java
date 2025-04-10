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
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluenceItem;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluencePaginationLinks;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluenceSearchResults;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluenceServerMetadata;
import org.opensearch.dataprepper.plugins.source.confluence.rest.ConfluenceRestClient;
import org.opensearch.dataprepper.plugins.source.confluence.utils.ConfluenceConfigHelper;
import org.opensearch.dataprepper.plugins.source.confluence.utils.ConfluenceContentType;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.springframework.util.CollectionUtils;

import javax.inject.Named;
import java.time.Instant;
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
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.GREATER_THAN;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.PREFIX;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.SPACE_IN;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.SPACE_NOT_IN;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.CqlConstants.SUFFIX;


/**
 * Service class for interactive external Atlassian Confluence SaaS service and fetch required details using their rest apis.
 */

@Slf4j
@Named
public class ConfluenceService {


    public static final String CONTENT_TYPE = "ContentType";
    public static final String CQL_LAST_MODIFIED_DATE_FORMAT = "yyyy-MM-dd HH:mm";
    private static final String SEARCH_RESULTS_FOUND = "searchResultsFound";
    private ZoneId confluenceServerZoneId = null;
    private final ConfluenceSourceConfig confluenceSourceConfig;
    private final ConfluenceRestClient confluenceRestClient;
    private final Counter searchResultsFoundCounter;


    public ConfluenceService(ConfluenceSourceConfig confluenceSourceConfig,
                             ConfluenceRestClient confluenceRestClient,
                             PluginMetrics pluginMetrics) {
        this.confluenceSourceConfig = confluenceSourceConfig;
        this.confluenceRestClient = confluenceRestClient;
        this.searchResultsFoundCounter = pluginMetrics.counter(SEARCH_RESULTS_FOUND);
    }

    /**
     * Get Confluence entities.
     *
     * @param configuration the configuration.
     * @param timestamp     timestamp.
     * @param itemInfoQueue queue for storing item information.
     */
    public void getPages(ConfluenceSourceConfig configuration, Instant timestamp, Queue<ItemInfo> itemInfoQueue) {
        log.trace("Started to fetch entities");
        searchForNewContentAndAddToQueue(configuration, timestamp, itemInfoQueue);
        log.trace("Creating item information and adding in queue");
    }

    public String getContent(String contentId) {
        return confluenceRestClient.getContent(contentId);
    }

    private void initializeConfluenceServerMetadata() {
        ConfluenceServerMetadata confluenceServerMetadata = confluenceRestClient.getConfluenceServerMetadata();
        this.confluenceServerZoneId = confluenceServerMetadata.getDefaultTimeZone();
    }

    /**
     * Method for building Content Item Info.
     *
     * @param configuration Input Parameter
     * @param timestamp     Input Parameter
     */
    private void searchForNewContentAndAddToQueue(ConfluenceSourceConfig configuration, Instant timestamp,
                                                  Queue<ItemInfo> itemInfoQueue) {
        log.trace("Looking for Add/Modified tickets with a Search API call");
        StringBuilder cql = createContentFilterCriteria(configuration, timestamp);
        int total = 0;
        int startAt = 0;
        ConfluencePaginationLinks paginationLinks = null;
        do {
            ConfluenceSearchResults searchContentItems = confluenceRestClient.getAllContent(cql, startAt, paginationLinks);
            List<ConfluenceItem> contentList = new ArrayList<>(searchContentItems.getResults());
            total += searchContentItems.getSize();
            startAt += searchContentItems.getResults().size();
            addItemsToQueue(contentList, itemInfoQueue);
            log.debug("Content items fetched so far: {}", total);
            paginationLinks = searchContentItems.getLinks();
            searchResultsFoundCounter.increment(searchContentItems.getSize());
        } while (paginationLinks != null && paginationLinks.getNext() != null);
        log.info("Number of content items found in search api call: {}", total);
    }

    /**
     * Add items to queue.
     *
     * @param contentList   Content list.
     * @param itemInfoQueue Item info queue.
     */
    private void addItemsToQueue(List<ConfluenceItem> contentList, Queue<ItemInfo> itemInfoQueue) {
        contentList.forEach(contentItem -> itemInfoQueue.add(ConfluenceItemInfo.builder()
                .withEventTime(Instant.now()).withContentBean(contentItem).build()));
    }


    /**
     * Method for creating Content Filter Criteria.
     * Made this method package private to be able to test with UnitTests
     *
     * @param configuration Input Parameter
     * @param ts            Input Parameter
     * @return String Builder
     */
    StringBuilder createContentFilterCriteria(ConfluenceSourceConfig configuration, Instant ts) {

        log.info("Creating content filter criteria");
        if (!CollectionUtils.isEmpty(ConfluenceConfigHelper.getSpacesNameIncludeFilter(configuration)) || !CollectionUtils.isEmpty(ConfluenceConfigHelper.getSpacesNameExcludeFilter(configuration))) {
            validateSpaceFilters(configuration);
        }

        if (!CollectionUtils.isEmpty(ConfluenceConfigHelper.getContentTypeIncludeFilter(configuration)) || !CollectionUtils.isEmpty(ConfluenceConfigHelper.getContentTypeExcludeFilter(configuration))) {
            validatePageTypeFilters(configuration);
        }

        if (this.confluenceServerZoneId == null) {
            // initialize confluence server timezone
            initializeConfluenceServerMetadata();
        }

        String formattedTimeStamp = ts.atZone(this.confluenceServerZoneId).format(DateTimeFormatter.ofPattern(CQL_LAST_MODIFIED_DATE_FORMAT));
        StringBuilder cQl = new StringBuilder(LAST_MODIFIED + GREATER_THAN + "\"" + formattedTimeStamp + "\"");
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
        cQl.append(" order by " + LAST_MODIFIED);
        log.info("Created content filter criteria ConfluenceQl query: {}", cQl);
        return cQl;
    }

    /**
     * Method for Validating Page Type Filters.
     *
     * @param configuration Input Parameter
     */
    private void validatePageTypeFilters(ConfluenceSourceConfig configuration) {
        log.trace("Validating Page Type filters");
        List<String> badFilters = new ArrayList<>();
        Set<String> includedPageType = new HashSet<>();
        List<String> includedAndExcludedPageType = new ArrayList<>();
        ConfluenceConfigHelper.getContentTypeIncludeFilter(configuration).forEach(pageTypeFilter -> {
            if (ConfluenceContentType.fromString(pageTypeFilter) == null) {
                badFilters.add(pageTypeFilter);
            } else {
                includedPageType.add(pageTypeFilter);
            }
        });
        ConfluenceConfigHelper.getContentTypeExcludeFilter(configuration).forEach(pageTypeFilter -> {
            if (includedPageType.contains(pageTypeFilter)) {
                includedAndExcludedPageType.add(pageTypeFilter);
            }
            if (ConfluenceContentType.fromString(pageTypeFilter) == null) {
                badFilters.add(pageTypeFilter);
            }
        });
        if (!badFilters.isEmpty()) {
            String filters = String.join("\"" + badFilters + "\"", ", ");
            log.error("One or more invalid Page Types found in filter configuration: {}", badFilters);
            throw new InvalidPluginConfigurationException("Bad request exception occurred " +
                    "Invalid Page Type key found in filter configuration "
                    + filters);
        }
        if (!includedAndExcludedPageType.isEmpty()) {
            String filters = String.join("\"" + includedAndExcludedPageType + "\"", ", ");
            log.error("One or more Page types found in both include and exclude: {}", includedAndExcludedPageType);
            throw new InvalidPluginConfigurationException("Bad request exception occurred " +
                    "Page Type filters is invalid because the following Page types are listed in both include and exclude"
                    + filters);
        }

    }

    /**
     * Method for Validating Space Filters.
     *
     * @param configuration Input Parameter
     */
    private void validateSpaceFilters(ConfluenceSourceConfig configuration) {
        log.trace("Validating space filters");
        List<String> badFilters = new ArrayList<>();
        Set<String> includedSpaces = new HashSet<>();
        List<String> includedAndExcludedSpaces = new ArrayList<>();
        Pattern regex = Pattern.compile("[^A-Z0-9]");
        ConfluenceConfigHelper.getSpacesNameIncludeFilter(configuration).forEach(spaceFilter -> {
            Matcher matcher = regex.matcher(spaceFilter);
            includedSpaces.add(spaceFilter);
            if (matcher.find() || spaceFilter.length() <= 1 || spaceFilter.length() > 100) {
                badFilters.add(spaceFilter);
            }
        });
        ConfluenceConfigHelper.getSpacesNameExcludeFilter(configuration).forEach(spaceFilter -> {
            Matcher matcher = regex.matcher(spaceFilter);
            if (includedSpaces.contains(spaceFilter)) {
                includedAndExcludedSpaces.add(spaceFilter);
            }
            if (matcher.find() || spaceFilter.length() <= 1 || spaceFilter.length() > 100) {
                badFilters.add(spaceFilter);
            }
        });
        if (!badFilters.isEmpty()) {
            String filters = String.join("\"" + badFilters + "\"", ", ");
            log.error("One or more invalid Space keys found in filter configuration: {}", badFilters);
            throw new InvalidPluginConfigurationException("Bad request exception occurred " +
                    "Invalid Space key found in filter configuration for "
                    + filters);
        }
        if (!includedAndExcludedSpaces.isEmpty()) {
            String filters = String.join("\"" + includedAndExcludedSpaces + "\"", ", ");
            log.error("One or more Space keys found in both include and exclude: {}", includedAndExcludedSpaces);
            throw new InvalidPluginConfigurationException("Bad request exception occurred " +
                    "Space filters is invalid because the following space are listed in both include and exclude"
                    + filters);
        }

    }

}