/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.opensearch.client.ClusterClient;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexTemplatesResponse;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.text.MatchesPattern.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ManagementDisabledIndexManagerTest {
    private IndexManagerFactory indexManagerFactory;
    private String baseIndexAlias;
    private String indexAliasWithTimePattern;

    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    private RestHighLevelClient restHighLevelClient;

    @Mock
    private OpenSearchSinkConfiguration openSearchSinkConfiguration;

    @Mock
    private TemplateStrategy templateStrategy;

    @Mock
    private ClusterSettingsParser clusterSettingsParser;

    @Mock
    private ClusterClient cluster;

    @Mock
    private ClusterGetSettingsResponse clusterGetSettingsResponse;

    @Mock
    private IndexConfiguration indexConfiguration;

    @Mock
    private IndicesClient indicesClient;

    @Mock
    private GetIndexTemplatesResponse getIndexTemplatesResponse;

    @Mock
    private RestClient restClient;

    @Mock
    private ResponseException responseException;


    @BeforeEach
    void setup() {
        baseIndexAlias = UUID.randomUUID().toString();
        indexAliasWithTimePattern = baseIndexAlias + "-%{yyyy.MM.dd.HH}";
        indexManagerFactory = new IndexManagerFactory(clusterSettingsParser);
        when(openSearchSinkConfiguration.getIndexConfiguration()).thenReturn(indexConfiguration);
        when(indexConfiguration.getIndexAlias()).thenReturn(baseIndexAlias);
    }

    @AfterEach
    void verifyMocksHaveNoUnnecessaryInteractions() {
        verifyNoMoreInteractions(
                restHighLevelClient,
                openSearchSinkConfiguration,
                cluster,
                clusterGetSettingsResponse,
                indexConfiguration,
                indicesClient,
                getIndexTemplatesResponse,
                restClient,
                responseException
        );
    }

    @Test
    void getIndexAlias_IndexWithTimePattern() {
        when(indexConfiguration.getIndexAlias()).thenReturn(indexAliasWithTimePattern);
        final IndexManager objectUnderTest = indexManagerFactory.getIndexManager(
                IndexType.MANAGEMENT_DISABLED, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        final Pattern expectedIndexPattern = Pattern.compile(baseIndexAlias + "-\\d{4}.\\d{2}.\\d{2}.\\d{2}");
        try {
            final String actualIndexPattern = objectUnderTest.getIndexName(null);
            assertThat(actualIndexPattern, matchesPattern(expectedIndexPattern));
        } catch (IOException e){}
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void getIndexAlias_IndexWithTimePattern_Exceptional_NotAsSuffix() {
        when(indexConfiguration.getIndexAlias()).thenReturn(indexAliasWithTimePattern + "-randomtext");
        final IndexManager objectUnderTest = indexManagerFactory.getIndexManager(
                IndexType.MANAGEMENT_DISABLED, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        final Pattern expectedIndexPattern = Pattern.compile(baseIndexAlias + "-\\d{4}.\\d{2}.\\d{2}.\\d{2}-randomtext");
        try {
            final String actualIndexPattern = objectUnderTest.getIndexName(null);
            assertThat(actualIndexPattern, matchesPattern(expectedIndexPattern));
        } catch (IOException e){}
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @ParameterizedTest
    @ValueSource(chars = {'#', '\\', '/', '*', '?', '"', '<', '>', '|', ',', ':'})
    void getIndexAlias_IndexWithTimePattern_Exceptional_WithSpecialChars(final char invalidCharacter) {
        when(indexConfiguration.getIndexAlias()).thenReturn(baseIndexAlias + "-%{yyyy" + invalidCharacter + ".MM.dd.HH}");
        assertThrows(IllegalArgumentException.class,
                () -> indexManagerFactory.getIndexManager(
                        IndexType.MANAGEMENT_DISABLED, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void testIndexTimePattern_Exceptional_MultipleTimePatterns() {
        when(indexConfiguration.getIndexAlias()).thenReturn(baseIndexAlias + "-%{yyyy}-%{MM.dd.HH}");
        assertThrows(IllegalArgumentException.class,
                () -> indexManagerFactory.getIndexManager(
                        IndexType.MANAGEMENT_DISABLED, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void testIndexTimePattern_Exceptional_NestedPatterns() {
        when(indexConfiguration.getIndexAlias()).thenReturn(baseIndexAlias + "-%{%{yyyy.MM.dd}}");
        assertThrows(IllegalArgumentException.class,
                () -> indexManagerFactory.getIndexManager(
                        IndexType.MANAGEMENT_DISABLED, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @ParameterizedTest
    @ValueSource(chars = {'m', 's', 'S', 'A', 'n', 'N'})
    void getIndexAlias_IndexWithTimePattern_TooGranular(final char granularTimePattern) {
        when(indexConfiguration.getIndexAlias()).thenReturn(baseIndexAlias + "-%{yyyy.MM.dd.HH." + granularTimePattern + "}");
        assertThrows(IllegalArgumentException.class,
                () -> indexManagerFactory.getIndexManager(
                        IndexType.MANAGEMENT_DISABLED, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void setupIndex_does_nothing() throws IOException {
        when(indexConfiguration.getIndexAlias()).thenReturn(indexAliasWithTimePattern);
        final IndexManager objectUnderTest = indexManagerFactory.getIndexManager(
                IndexType.MANAGEMENT_DISABLED, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);

        verifyNoMoreInteractions(
                restHighLevelClient,
                openSearchSinkConfiguration,
                cluster,
                clusterGetSettingsResponse,
                indexConfiguration,
                indicesClient,
                getIndexTemplatesResponse,
                restClient,
                responseException
        );
    }
}
