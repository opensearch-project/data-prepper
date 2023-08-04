package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.plugins.sink.opensearch.DistributionVersion;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IndexTemplateAPIWrapperFactoryTest {
    @Mock
    private IndexConfiguration indexConfiguration;

    @Mock
    private OpenSearchClient openSearchClient;

    @Test
    void testGetEs6IndexTemplateAPIWrapper() {
        when(indexConfiguration.getDistributionVersion()).thenReturn(DistributionVersion.ES6);
        assertThat(IndexTemplateAPIWrapperFactory.getWrapper(indexConfiguration, openSearchClient),
                instanceOf(Es6IndexTemplateAPIWrapper.class));
    }

    @Test
    void testGetOpenSearchLegacyTemplateAPIWrapper() {
        when(indexConfiguration.getDistributionVersion()).thenReturn(DistributionVersion.DEFAULT);
        when(indexConfiguration.getTemplateType()).thenReturn(TemplateType.V1);
        assertThat(IndexTemplateAPIWrapperFactory.getWrapper(indexConfiguration, openSearchClient),
                instanceOf(OpenSearchLegacyTemplateAPIWrapper.class));
    }

    @Test
    void testGetComposableTemplateAPIWrapper() {
        when(indexConfiguration.getDistributionVersion()).thenReturn(DistributionVersion.DEFAULT);
        when(indexConfiguration.getTemplateType()).thenReturn(TemplateType.INDEX_TEMPLATE);
        assertThat(IndexTemplateAPIWrapperFactory.getWrapper(indexConfiguration, openSearchClient),
                instanceOf(ComposableTemplateAPIWrapper.class));
    }
}