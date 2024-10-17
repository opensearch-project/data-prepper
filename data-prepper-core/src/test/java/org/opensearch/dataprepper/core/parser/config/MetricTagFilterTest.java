/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.parser.config.MetricTagFilter;

import java.util.Map;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@ExtendWith(MockitoExtension.class)
class MetricTagFilterTest {

    @Test
    void testMetricTagFilterGetters() {
        final String testRegex = ".*";
        final Map<String, String> testTags = Map.of("key", "value");
        final MetricTagFilter metricTagFilter = new MetricTagFilter(testRegex, testTags);

        assertThat(metricTagFilter, notNullValue());
        assertThat(metricTagFilter.getPattern(), equalTo(testRegex));
        assertThat(metricTagFilter.getTags().size(), equalTo(testTags.size()));
        assertThat(metricTagFilter.getTags(), equalTo(testTags));
    }

}