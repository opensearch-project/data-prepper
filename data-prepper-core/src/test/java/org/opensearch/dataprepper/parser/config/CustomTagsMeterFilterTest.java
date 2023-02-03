/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.config;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.opensearch.dataprepper.DataPrepper.getServiceNameForMetrics;
import static org.opensearch.dataprepper.metrics.MetricNames.SERVICE_NAME;

class CustomTagsMeterFilterTest {
    private static final String testTagKey = "testKey";
    final String testFilterKey = "testFilterKey";
    final String testTagValue = "testValue";
    final String testFilterValue = "testFilterValue";

    CustomTagsMeterFilter createObjectUnderTest(final String regexPattern) {
        final MetricTagFilter metricTagFilter = new MetricTagFilter(regexPattern, Map.of(testFilterKey, testFilterValue));
        return new CustomTagsMeterFilter(Map.of(testTagKey, testTagValue), List.of(metricTagFilter));
    }

    @Test
    void testMapShouldAddTagsIfRegexMatches() {
        final CustomTagsMeterFilter objectUnderTest = createObjectUnderTest("name.*");

        final Meter.Id testMeterId = new Meter.Id("name", Tags.empty(), null, null, Meter.Type.COUNTER);
        final Meter.Id testMeterIdWithTags = objectUnderTest.map(testMeterId);

        final List<Tag> expectedTags = List.of(Tag.of(SERVICE_NAME, getServiceNameForMetrics()), Tag.of(testFilterKey, testFilterValue));

        assertThat(testMeterIdWithTags, notNullValue());
        System.out.println(testMeterIdWithTags);
        assertThat(testMeterIdWithTags.getTags().size(), equalTo(2));
        assertThat(testMeterIdWithTags.getTags(), equalTo(expectedTags));
    }

    @Test
    void testMapShouldAddTagsFromMetricTagsIfRegexDoesNotMatch() {
        final CustomTagsMeterFilter objectUnderTest = createObjectUnderTest("nomatch.*");

        final Meter.Id testMeterId = new Meter.Id("name", Tags.empty(), null, null, Meter.Type.COUNTER);
        final Meter.Id testMeterIdWithTags = objectUnderTest.map(testMeterId);

        final List<Tag> expectedTags = List.of(Tag.of(SERVICE_NAME, getServiceNameForMetrics()), Tag.of(testTagKey, testTagValue));

        assertThat(testMeterIdWithTags, notNullValue());
        System.out.println(testMeterIdWithTags);
        assertThat(testMeterIdWithTags.getTags().size(), equalTo(2));
        assertThat(testMeterIdWithTags.getTags(), equalTo(expectedTags));
    }

}