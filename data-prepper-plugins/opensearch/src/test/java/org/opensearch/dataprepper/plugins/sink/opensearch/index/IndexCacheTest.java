/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

class IndexCacheTest {
    
    private IndexCache indexCache;
    
    @BeforeEach
    void setUp() {
        indexCache = new IndexCache();
    }
    
    @Test
    void putAndGetDataStreamResult_storesAndRetrievesCorrectly() {
        indexCache.putDataStreamResult("my-data-stream", true);
        indexCache.putDataStreamResult("regular-index", false);
        
        assertThat(indexCache.getDataStreamResult("my-data-stream"), equalTo(true));
        assertThat(indexCache.getDataStreamResult("regular-index"), equalTo(false));
    }
    
    @Test
    void getDataStreamResult_returnsNull_whenNotCached() {
        assertThat(indexCache.getDataStreamResult("unknown-index"), nullValue());
    }
    
    @Test
    void clearAll_removesAllCachedData() {
        indexCache.putDataStreamResult("my-data-stream", true);
        indexCache.putTimeSeriesTimestampField("my-timeseries-index", "event-time");
        
        indexCache.clearAll();
        
        assertThat(indexCache.getDataStreamResult("my-data-stream"), nullValue());
        assertThat(indexCache.isTimeSeriesStatusKnown("my-timeseries-index"), equalTo(false));
    }

    @Test
    void putAndGetTimeSeriesTimestampField_storesAndRetrievesCorrectly() {
        final String timestampField = UUID.randomUUID().toString();
        indexCache.putTimeSeriesTimestampField("my-timeseries-index", timestampField);
        indexCache.putTimeSeriesTimestampField("regular-index", null);

        assertThat(indexCache.getTimeSeriesTimestampField("my-timeseries-index"), equalTo(Optional.of(timestampField)));
        assertThat(indexCache.isTimeSeriesStatusKnown("my-timeseries-index"), equalTo(true));
        assertThat(indexCache.getTimeSeriesTimestampField("regular-index"), equalTo(Optional.empty()));
        assertThat(indexCache.isTimeSeriesStatusKnown("regular-index"), equalTo(true));
        assertThat(indexCache.getTimeSeriesTimestampField("unknown-index"), equalTo(Optional.empty()));
        assertThat(indexCache.isTimeSeriesStatusKnown("unknown-index"), equalTo(false));
    }

    @Test
    void putTimeSeriesTimestampField_replaces_non_time_series_result() {
        final String timestampField = UUID.randomUUID().toString();
        indexCache.putTimeSeriesTimestampField("my-index", null);
        indexCache.putTimeSeriesTimestampField("my-index", timestampField);

        assertThat(indexCache.getTimeSeriesTimestampField("my-index"), equalTo(Optional.of(timestampField)));
    }
}