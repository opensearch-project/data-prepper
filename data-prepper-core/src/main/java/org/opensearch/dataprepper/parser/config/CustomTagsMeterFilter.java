/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.config;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.config.MeterFilter;
import org.springframework.util.AntPathMatcher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.DataPrepper.getServiceNameForMetrics;
import static org.opensearch.dataprepper.metrics.MetricNames.DELIMITER;
import static org.opensearch.dataprepper.metrics.MetricNames.SERVICE_NAME;

public class CustomTagsMeterFilter implements MeterFilter {
    private static final AntPathMatcher ANT_PATH_MATCHER = new AntPathMatcher(DELIMITER);

    private final List<MetricTagFilter> metricTagFilters;
    private final Map<String, String> metricTagsWithServiceName;

    public CustomTagsMeterFilter(final Map<String, String> metricTags, final List<MetricTagFilter> metricTagFilters) {
        this.metricTagFilters = metricTagFilters;

        metricTagsWithServiceName = new HashMap<>(metricTags);
        metricTagsWithServiceName.putIfAbsent(SERVICE_NAME, getServiceNameForMetrics());
    }

    @Override
    public Meter.Id map(final Meter.Id id) {
        for (MetricTagFilter metricTagFilter: metricTagFilters) {
            final String metricRegex = metricTagFilter.getPattern();
            final Map<String, String> metricFilterTagsWithServiceName = new HashMap<>(metricTagFilter.getTags());
            metricFilterTagsWithServiceName.putIfAbsent(SERVICE_NAME, getServiceNameForMetrics());

            if (ANT_PATH_MATCHER.match(metricRegex, id.getName())) {
                return MeterFilter.commonTags(metricFilterTagsWithServiceName.entrySet().stream().map(e -> Tag.of(e.getKey(), e.getValue()))
                        .collect(Collectors.toList())).map(id);
            }
        }
        return MeterFilter.commonTags(metricTagsWithServiceName.entrySet().stream().map(e -> Tag.of(e.getKey(), e.getValue()))
                .collect(Collectors.toList())).map(id);
    }
}
