/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
        
        indexCache.clearAll();
        
        assertThat(indexCache.getDataStreamResult("my-data-stream"), nullValue());
    }
}