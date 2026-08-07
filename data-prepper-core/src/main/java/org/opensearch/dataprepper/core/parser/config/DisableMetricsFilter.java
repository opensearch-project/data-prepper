/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.parser.config;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.AntPathMatcher;
import static org.opensearch.dataprepper.metrics.MetricNames.DELIMITER;


import java.util.Collections;
import java.util.List;

public class DisableMetricsFilter implements MeterFilter {
    private static final Logger LOG = LoggerFactory.getLogger(DisableMetricsFilter.class);
    private final List<String> disabledPatterns;
    private static final AntPathMatcher matcher = new AntPathMatcher(DELIMITER);

    public DisableMetricsFilter(final List<String> disabledPatterns) {
        this.disabledPatterns = disabledPatterns != null ? disabledPatterns : Collections.emptyList();
    }

    @Override
    public MeterFilterReply accept(final Meter.Id id) {
        final String metricName = id.getName();

        for (final String pattern : disabledPatterns) {
            if (matcher.match(pattern, metricName)) {
                return MeterFilterReply.DENY;
            }
        }
        return MeterFilterReply.NEUTRAL;
    }

}
