/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SortConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SortingOptions {

    private static final Logger LOG = LoggerFactory.getLogger(SortingOptions.class);

    private String fieldName;

    @JsonProperty("order")
    private String order = "asc";

    @JsonProperty("format")
    private String format;

    @JsonProperty("mode")
    private String mode;

    public String getFieldName() {
        return fieldName;
    }

    public String getOrder() {
        return order;
    }

    public String getFormat() {
        return format;
    }

    public String getMode() {
        return mode;
    }

    public static List<SortingOptions> fromSortConfigs(final List<SortConfig> sortConfigs) {
        if (sortConfigs == null || sortConfigs.isEmpty()) {
            return Collections.emptyList();
        }
        final List<SortingOptions> result = sortConfigs.stream()
                .map(SortingOptions::fromSortConfig)
                .collect(Collectors.toList());

        final boolean hasIdField = result.stream()
                .anyMatch(opt -> "_id".equals(opt.getFieldName()));
        if (!hasIdField) {
            LOG.warn("Custom sort does not include _id. Pagination results may contain duplicates " +
                    "or miss documents when sort values are not unique.");
        }

        return result;
    }

    private static SortingOptions fromSortConfig(final SortConfig sortConfig) {
        final SortingOptions sortingOptions = new SortingOptions();
        sortingOptions.fieldName = sortConfig.getName();
        sortingOptions.order = sortConfig.getOrder().getSortOrderValue();
        return sortingOptions;
    }
}