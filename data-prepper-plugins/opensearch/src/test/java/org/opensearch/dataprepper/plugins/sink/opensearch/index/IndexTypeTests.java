/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;


public class IndexTypeTests {

    @Test
    public void getByValue() {
        assertEquals(Optional.empty(), IndexType.getByValue(null));
        assertEquals(Optional.empty(), IndexType.getByValue("illegal-index-type"));
        assertEquals(Optional.of(IndexType.CUSTOM), IndexType.getByValue("custom"));
        assertEquals(Optional.of(IndexType.MANAGEMENT_DISABLED), IndexType.getByValue("management_disabled"));
        assertEquals(Optional.of(IndexType.TRACE_ANALYTICS_RAW), IndexType.getByValue("trace-analytics-raw"));
        assertEquals(Optional.of(IndexType.TRACE_ANALYTICS_RAW_STANDARD), IndexType.getByValue("trace-analytics-standard-raw"));
        assertEquals(Optional.of(IndexType.TRACE_ANALYTICS_SERVICE_MAP), IndexType.getByValue("trace-analytics-service-map"));
        assertEquals(Optional.of(IndexType.LOG_ANALYTICS), IndexType.getByValue("log-analytics"));
        assertEquals(Optional.of(IndexType.LOG_ANALYTICS_STANDARD), IndexType.getByValue("log-analytics-standard"));
        assertEquals(Optional.of(IndexType.METRIC_ANALYTICS), IndexType.getByValue("metric-analytics"));
        assertEquals(Optional.of(IndexType.METRIC_ANALYTICS_STANDARD), IndexType.getByValue("metric-analytics-standard"));
    }

    @Test
    public void getIndexTypeValues() {
        assertEquals("[trace-analytics-raw, trace-analytics-standard-raw, trace-analytics-service-map, log-analytics, log-analytics-standard, metric-analytics, metric-analytics-standard, custom, management_disabled]", IndexType.getIndexTypeValues());
    }

    @ParameterizedTest
    @EnumSource(IndexType.class)
    void getByValue_on_IndexType_getValue_returns_the_IndexType(IndexType indexType) {
        final Optional<IndexType> optionReturnedIndex = IndexType.getByValue(indexType.getValue());
        assertThat(optionReturnedIndex, notNullValue());
        assertThat(optionReturnedIndex.isPresent(), equalTo(true));
        assertThat(optionReturnedIndex.get(), equalTo(indexType));
    }
}
