/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink.opensearch.index;

import com.amazon.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.opensearch.client.RestHighLevelClient;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class IndexManagerFactoryTests {

    private static final String INDEX_ALIAS = "test-index-alias";
    private IndexManagerFactory indexManagerFactory;

    @Mock
    private RestHighLevelClient restHighLevelClient;

    @Mock
    private OpenSearchSinkConfiguration openSearchSinkConfiguration;

    @Mock
    private IndexConfiguration indexConfiguration;

    @Before
    public void setup() {
        initMocks(this);
        when(openSearchSinkConfiguration.getIndexConfiguration()).thenReturn(indexConfiguration);
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS);
        indexManagerFactory = new IndexManagerFactory();
    }

    @Test
    public void getIndexManager_TraceAnalyticsRaw() {
        final IndexManager indexManager =
                indexManagerFactory.getIndexManager(IndexType.TRACE_ANALYTICS_RAW, restHighLevelClient, openSearchSinkConfiguration);
        assertThat(indexManager, instanceOf(IndexManager.class));
    }

    @Test
    public void getIndexManager_TraceAnalyticsServiceMap() {
        final IndexManager indexManager =
                indexManagerFactory.getIndexManager(IndexType.TRACE_ANALYTICS_SERVICE_MAP, restHighLevelClient, openSearchSinkConfiguration);
        assertThat(indexManager, instanceOf(IndexManager.class));
    }

    @Test
    public void getIndexManager_Default() {
        final IndexManager indexManager =
                indexManagerFactory.getIndexManager(IndexType.CUSTOM, restHighLevelClient, openSearchSinkConfiguration);
        assertThat(indexManager, instanceOf(IndexManager.class));
    }

}
