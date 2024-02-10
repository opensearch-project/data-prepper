package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.plugins.sink.opensearch.DistributionVersion;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BulkApiWrapperFactoryTest {
    @Mock
    private IndexConfiguration indexConfiguration;

    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    private RestHighLevelClient restHighLevelClient;

    @Test
    void testGetEs6BulkApiWrapper() {
        when(indexConfiguration.getDistributionVersion()).thenReturn(DistributionVersion.ES6);
        assertThat(BulkApiWrapperFactory.getWrapper(indexConfiguration, openSearchClient, restHighLevelClient),
                instanceOf(Es6BulkApiWrapper.class));
    }

    @Test
    void testGetOpenSearchDefaultBulkApiWrapper() {
        when(indexConfiguration.getDistributionVersion()).thenReturn(DistributionVersion.DEFAULT);
        assertThat(BulkApiWrapperFactory.getWrapper(indexConfiguration, openSearchClient, restHighLevelClient),
                instanceOf(OpenSearchDefaultBulkApiWrapper.class));
    }

    @Test
    void testGetEnableDetectorScanBulkApiWrapper() {
        when(indexConfiguration.isEnableDetectorScan()).thenReturn(true);
        assertThat(BulkApiWrapperFactory.getWrapper(indexConfiguration, openSearchClient, restHighLevelClient),
                instanceOf(OpenSearchDetectorScanBulkApiWrapper.class));
    }
}