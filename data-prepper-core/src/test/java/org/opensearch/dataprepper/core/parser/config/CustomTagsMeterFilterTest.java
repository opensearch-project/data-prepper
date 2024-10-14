/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser.config;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.core.parser.config.CustomTagsMeterFilter;
import org.opensearch.dataprepper.core.parser.config.MetricTagFilter;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.opensearch.dataprepper.DataPrepper.getServiceNameForMetrics;
import static org.opensearch.dataprepper.metrics.MetricNames.SERVICE_NAME;

class CustomTagsMeterFilterTest {
    private static final String testTagKey = "testKey";
    private final String testFilterKey = "testFilterKey";
    private final String testTagValue = "testValue";
    private final String testFilterValue = "testFilterValue";
    private final String testMetricName = "test-pipeline.grok.matchErrors.count";

    private CustomTagsMeterFilter createObjectUnderTest(final String regexPattern) {
        final MetricTagFilter metricTagFilter = new MetricTagFilter(regexPattern, Map.of(testFilterKey, testFilterValue));
        return new CustomTagsMeterFilter(Map.of(testTagKey, testTagValue), List.of(metricTagFilter));
    }

    @ParameterizedTest
    @ValueSource(strings = {"test-pipeline.grok.**", "**.grok.**", "**.*Errors.*", "**.count", "**"})
    void testMapShouldAddTagsIfRegexMatches(final String regex) {
        final CustomTagsMeterFilter objectUnderTest = createObjectUnderTest(regex);

        final Meter.Id testMeterId = new Meter.Id(testMetricName, Tags.empty(), null, null, Meter.Type.COUNTER);
        final Meter.Id testMeterIdWithTags = objectUnderTest.map(testMeterId);

        final List<Tag> expectedTags = List.of(Tag.of(testFilterKey, testFilterValue));

        assertThat(testMeterIdWithTags, notNullValue());
        assertThat(testMeterIdWithTags.getTags(), allOf(
                hasSize(1),
                equalTo(expectedTags)
        ));
    }

    @ParameterizedTest
    @ValueSource(strings = {"test-pipeline.grok**", "**.?grok.**", "**.Errors.*", "*.count"})
    void testMapShouldAddTagsFromMetricTagsIfRegexDoesNotMatch(final String regex) {
        final CustomTagsMeterFilter objectUnderTest = createObjectUnderTest(regex);

        final Meter.Id testMeterId = new Meter.Id(testMetricName, Tags.empty(), null, null, Meter.Type.COUNTER);
        final Meter.Id testMeterIdWithTags = objectUnderTest.map(testMeterId);

        final List<Tag> expectedTags = List.of(Tag.of(SERVICE_NAME, getServiceNameForMetrics()), Tag.of(testTagKey, testTagValue));

        assertThat(testMeterIdWithTags, notNullValue());
        assertThat(testMeterIdWithTags.getTags(), allOf(
                hasSize(2),
                equalTo(expectedTags)
        ));
    }

}