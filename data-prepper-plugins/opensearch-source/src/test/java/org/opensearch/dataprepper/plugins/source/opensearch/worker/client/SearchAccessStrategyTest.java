/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchVersionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchVersionInfo;
import org.opensearch.client.opensearch.core.InfoResponse;
import org.opensearch.client.util.MissingRequiredPropertyException;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.AwsAuthenticationConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SearchConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchContextType;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessorStrategy.OPENSEARCH_DISTRIBUTION;

@ExtendWith(MockitoExtension.class)
public class SearchAccessStrategyTest {

    @Mock
    private OpenSearchClientFactory openSearchClientFactory;

    @Mock
    private OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @Mock
    private PluginConfigObservable pluginConfigObservable;

    private SearchAccessorStrategy createObjectUnderTest() {
        return SearchAccessorStrategy.create(
                openSearchSourceConfiguration, openSearchClientFactory, pluginConfigObservable);
    }

    @ParameterizedTest
    @ValueSource(strings = {"2.5.0", "2.6.1", "3.0.0"})
    void testHappyPath_for_different_point_in_time_versions_for_opensearch(final String osVersion) throws IOException {

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getSearchContextType()).thenReturn(null);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        final InfoResponse infoResponse = mock(InfoResponse.class);
        final OpenSearchVersionInfo openSearchVersionInfo = mock(OpenSearchVersionInfo.class);
        when(openSearchVersionInfo.distribution()).thenReturn(OPENSEARCH_DISTRIBUTION);
        when(openSearchVersionInfo.number()).thenReturn(osVersion);
        when(infoResponse.version()).thenReturn(openSearchVersionInfo);

        final OpenSearchClient openSearchClient = mock(OpenSearchClient.class);
        when(openSearchClient.info()).thenReturn(infoResponse);
        when(openSearchClientFactory.provideOpenSearchClient(openSearchSourceConfiguration)).thenReturn(openSearchClient);
        final SearchAccessor searchAccessor = createObjectUnderTest().getSearchAccessor();
        assertThat(searchAccessor, notNullValue());
        assertThat(searchAccessor.getSearchContextType(), equalTo(SearchContextType.POINT_IN_TIME));
        verify(pluginConfigObservable).addPluginConfigObserver(any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"7.10.2", "8.1.1", "7.10.0"})
    void testHappyPath_for_different_point_in_time_versions_for_elasticsearch(final String esVersion) throws IOException {

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getSearchContextType()).thenReturn(null);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        final OpenSearchClient openSearchClient = mock(OpenSearchClient.class);
        when(openSearchClient.info()).thenThrow(MissingRequiredPropertyException.class);
        when(openSearchClientFactory.provideOpenSearchClient(openSearchSourceConfiguration)).thenReturn(openSearchClient);

        final ElasticsearchClient elasticsearchClient = mock(ElasticsearchClient.class);

        final co.elastic.clients.elasticsearch.core.InfoResponse infoResponse = mock(co.elastic.clients.elasticsearch.core.InfoResponse.class);
        final ElasticsearchVersionInfo elasticsearchVersionInfo = mock(ElasticsearchVersionInfo.class);
        when(elasticsearchVersionInfo.buildFlavor()).thenReturn("default");
        when(elasticsearchVersionInfo.number()).thenReturn(esVersion);
        when(infoResponse.version()).thenReturn(elasticsearchVersionInfo);

        when(elasticsearchClient.info()).thenReturn(infoResponse);
        when(openSearchClientFactory.provideElasticSearchClient(openSearchSourceConfiguration)).thenReturn(elasticsearchClient);

        final SearchAccessor searchAccessor = createObjectUnderTest().getSearchAccessor();
        assertThat(searchAccessor, notNullValue());
        assertThat(searchAccessor.getSearchContextType(), equalTo(SearchContextType.POINT_IN_TIME));
        verify(pluginConfigObservable).addPluginConfigObserver(any());
    }

    @ParameterizedTest
    @CsvSource(value = {"6.3.0,default", "7.9.0,default", "0.3.2,default", "7.10.2,oss"})
    void search_context_type_set_to_point_in_time_with_invalid_version_throws_IllegalArgumentException_for_elasticsearch(final String esVersion, final String esBuildFlavor) throws IOException {

        final OpenSearchClient openSearchClient = mock(OpenSearchClient.class);
        when(openSearchClient.info()).thenThrow(MissingRequiredPropertyException.class);
        when(openSearchClientFactory.provideOpenSearchClient(openSearchSourceConfiguration)).thenReturn(openSearchClient);

        final ElasticsearchClient elasticsearchClient = mock(ElasticsearchClient.class);

        final co.elastic.clients.elasticsearch.core.InfoResponse infoResponse = mock(co.elastic.clients.elasticsearch.core.InfoResponse.class);
        final ElasticsearchVersionInfo elasticsearchVersionInfo = mock(ElasticsearchVersionInfo.class);
        when(elasticsearchVersionInfo.buildFlavor()).thenReturn(esBuildFlavor);
        when(elasticsearchVersionInfo.number()).thenReturn(esVersion);
        when(infoResponse.version()).thenReturn(elasticsearchVersionInfo);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getSearchContextType()).thenReturn(SearchContextType.POINT_IN_TIME);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        when(elasticsearchClient.info()).thenReturn(infoResponse);
        when(openSearchClientFactory.provideElasticSearchClient(openSearchSourceConfiguration)).thenReturn(elasticsearchClient);


        assertThrows(IllegalArgumentException.class, () -> createObjectUnderTest().getSearchAccessor());
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.3.0", "2.4.9", "0.3.2"})
    void testHappyPath_with_for_different_scroll_versions_for_opensearch(final String osVersion) throws IOException {

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getSearchContextType()).thenReturn(null);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        final InfoResponse infoResponse = mock(InfoResponse.class);
        final OpenSearchVersionInfo openSearchVersionInfo = mock(OpenSearchVersionInfo.class);
        when(openSearchVersionInfo.distribution()).thenReturn(OPENSEARCH_DISTRIBUTION);
        when(openSearchVersionInfo.number()).thenReturn(osVersion);
        when(infoResponse.version()).thenReturn(openSearchVersionInfo);

        final OpenSearchClient openSearchClient = mock(OpenSearchClient.class);
        when(openSearchClient.info()).thenReturn(infoResponse);
        when(openSearchClientFactory.provideOpenSearchClient(openSearchSourceConfiguration)).thenReturn(openSearchClient);

        final SearchAccessor searchAccessor = createObjectUnderTest().getSearchAccessor();
        assertThat(searchAccessor, notNullValue());
        assertThat(searchAccessor.getSearchContextType(), equalTo(SearchContextType.SCROLL));
        verify(pluginConfigObservable).addPluginConfigObserver(any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.3.0", "2.4.9", "0.3.2"})
    void search_context_type_set_to_point_in_time_with_invalid_version_throws_IllegalArgumentException_for_opensearch(final String osVersion) throws IOException {

        final InfoResponse infoResponse = mock(InfoResponse.class);
        final OpenSearchVersionInfo openSearchVersionInfo = mock(OpenSearchVersionInfo.class);
        when(openSearchVersionInfo.distribution()).thenReturn(OPENSEARCH_DISTRIBUTION);
        when(openSearchVersionInfo.number()).thenReturn(osVersion);
        when(infoResponse.version()).thenReturn(openSearchVersionInfo);

        final OpenSearchClient openSearchClient = mock(OpenSearchClient.class);
        when(openSearchClient.info()).thenReturn(infoResponse);
        when(openSearchClientFactory.provideOpenSearchClient(openSearchSourceConfiguration)).thenReturn(openSearchClient);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getSearchContextType()).thenReturn(SearchContextType.POINT_IN_TIME);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        assertThrows(IllegalArgumentException.class, () -> createObjectUnderTest().getSearchAccessor());
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.3.0", "2.4.9", "2.5.0"})
    void search_context_type_set_to_none_uses_that_search_context_regardless_of_version(final String osVersion) throws IOException {

        final InfoResponse infoResponse = mock(InfoResponse.class);
        final OpenSearchVersionInfo openSearchVersionInfo = mock(OpenSearchVersionInfo.class);
        when(openSearchVersionInfo.distribution()).thenReturn(OPENSEARCH_DISTRIBUTION);
        when(openSearchVersionInfo.number()).thenReturn(osVersion);
        when(infoResponse.version()).thenReturn(openSearchVersionInfo);

        final OpenSearchClient openSearchClient = mock(OpenSearchClient.class);
        when(openSearchClient.info()).thenReturn(infoResponse);
        when(openSearchClientFactory.provideOpenSearchClient(openSearchSourceConfiguration)).thenReturn(openSearchClient);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getSearchContextType()).thenReturn(SearchContextType.NONE);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        final SearchAccessor searchAccessor = createObjectUnderTest().getSearchAccessor();
        assertThat(searchAccessor, notNullValue());
        assertThat(searchAccessor.getSearchContextType(), equalTo(SearchContextType.NONE));
        verify(pluginConfigObservable).addPluginConfigObserver(any());
    }

    @Test
    void serverless_flag_true_defaults_to_search_context_type_none() {

        final AwsAuthenticationConfiguration awsAuthenticationConfiguration = mock(AwsAuthenticationConfiguration.class);
        when(awsAuthenticationConfiguration.isServerlessCollection()).thenReturn(true);
        when(openSearchSourceConfiguration.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationConfiguration);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        final SearchAccessor searchAccessor = createObjectUnderTest().getSearchAccessor();

        assertThat(searchAccessor, notNullValue());
        assertThat(searchAccessor.getSearchContextType(), equalTo(SearchContextType.NONE));
        verifyNoInteractions(pluginConfigObservable);
    }

    @ParameterizedTest
    @ValueSource(strings = {"POINT_IN_TIME", "SCROLL"})
    void serverless_flag_true_throws_InvalidPluginConfiguration_if_search_context_type_is_point_in_time_or_scroll(final String searchContextType) {

        final AwsAuthenticationConfiguration awsAuthenticationConfiguration = mock(AwsAuthenticationConfiguration.class);
        when(awsAuthenticationConfiguration.isServerlessCollection()).thenReturn(true);
        when(openSearchSourceConfiguration.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationConfiguration);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getSearchContextType()).thenReturn(SearchContextType.valueOf(searchContextType));
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        final SearchAccessorStrategy objectUnderTest = createObjectUnderTest();

        assertThrows(InvalidPluginConfigurationException.class, objectUnderTest::getSearchAccessor);
    }

    @ParameterizedTest
    @ValueSource(strings = {"NONE"})
    void serverless_flag_true_uses_search_context_type_from_config(final String searchContextType) {

        final AwsAuthenticationConfiguration awsAuthenticationConfiguration = mock(AwsAuthenticationConfiguration.class);
        when(awsAuthenticationConfiguration.isServerlessCollection()).thenReturn(true);
        when(openSearchSourceConfiguration.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationConfiguration);

        final SearchConfiguration searchConfiguration = mock(SearchConfiguration.class);
        when(searchConfiguration.getSearchContextType()).thenReturn(SearchContextType.valueOf(searchContextType));
        when(openSearchSourceConfiguration.getSearchConfiguration()).thenReturn(searchConfiguration);

        final SearchAccessor searchAccessor = createObjectUnderTest().getSearchAccessor();

        assertThat(searchAccessor, notNullValue());
        assertThat(searchAccessor.getSearchContextType(), equalTo(SearchContextType.valueOf(searchContextType)));
        verifyNoInteractions(pluginConfigObservable);
    }
}
