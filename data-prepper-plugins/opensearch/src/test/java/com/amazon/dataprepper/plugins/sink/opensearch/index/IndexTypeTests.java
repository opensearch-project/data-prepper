/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink.opensearch.index;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;


public class IndexTypeTests {

    @Test
    public void getByValue() {
        assertEquals(Optional.empty(), IndexType.getByValue(null));
        assertEquals(Optional.empty(), IndexType.getByValue("illegal-index-type"));
        assertEquals(Optional.of(IndexType.CUSTOM), IndexType.getByValue("custom"));
        assertEquals(Optional.of(IndexType.TRACE_ANALYTICS_RAW), IndexType.getByValue("trace-analytics-raw"));
        assertEquals(Optional.of(IndexType.TRACE_ANALYTICS_SERVICE_MAP), IndexType.getByValue("trace-analytics-service-map"));
    }

    @Test
    public void getIndexTypeValues() {
        assertEquals("[trace-analytics-raw, trace-analytics-service-map, custom]", IndexType.getIndexTypeValues());
    }

}
