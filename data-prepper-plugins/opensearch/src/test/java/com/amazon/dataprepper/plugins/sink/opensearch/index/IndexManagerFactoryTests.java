package com.amazon.dataprepper.plugins.sink.opensearch.index;

import com.amazon.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.opensearch.client.RestHighLevelClient;

import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;

public class IndexManagerFactoryTests {

    private IndexManagerFactory indexManagerFactory;

    @Mock
    private RestHighLevelClient restHighLevelClient;

    @Mock
    private OpenSearchSinkConfiguration openSearchSinkConfiguration;

    @Before
    public void setup() {
        initMocks(this);
        indexManagerFactory = new IndexManagerFactory();
    }

    @Test
    public void getIndexManager_TraceAnalyticsRaw() {
        final IndexManager indexManager =
                indexManagerFactory.getIndexManager(IndexType.TRACE_ANALYTICS_RAW, restHighLevelClient, openSearchSinkConfiguration);
        assertEquals(TraceAnalyticsRawIndexManager.class, indexManager.getClass());
    }

    @Test
    public void getIndexManager_TraceAnalyticsServiceMap() {
        final IndexManager indexManager =
                indexManagerFactory.getIndexManager(IndexType.TRACE_ANALYTICS_SERVICE_MAP, restHighLevelClient, openSearchSinkConfiguration);
        assertEquals(TraceAnalyticsServiceMapIndexManager.class, indexManager.getClass());
    }

    @Test
    public void getIndexManager_Default() {
        final IndexManager indexManager =
                indexManagerFactory.getIndexManager(IndexType.CUSTOM, restHighLevelClient, openSearchSinkConfiguration);
        assertEquals(DefaultIndexManager.class, indexManager.getClass());
    }
}
