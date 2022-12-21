/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.opensearch.client.ClusterClient;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexTemplatesResponse;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.time.format.DateTimeFormatter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class DynamicIndexManagerTests {

    private static final String ID = "id";
    private static final String DYNAMIC = "dynamic";
    private static final String NEW_DYNAMIC = "new-dynamic";
    private static final String INDEX_ALIAS = "test-${" + ID + "}-index-alias";
    private static final String DATE_PATTERN = "yyyy.MM.dd";
    private static final String INDEX_ALIAS_WITH_DATE_PATTERN = INDEX_ALIAS+ "-%{" + DATE_PATTERN + "}";

    @Mock
    private IndexManagerFactory mockIndexManagerFactory;
    private IndexManagerFactory indexManagerFactory;

    @Mock
    private IndexManager indexManager;

    private DynamicIndexManager dynamicIndexManager;

    private IndexManager innerIndexManager;

    @Mock
    private RestHighLevelClient restHighLevelClient;

    @Mock
    private OpenSearchSinkConfiguration openSearchSinkConfiguration;

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

    static final String EVENT_TYPE = "event";

    @BeforeEach
    public void setup() throws IOException {
        initMocks(this);

        indexManagerFactory = new IndexManagerFactory();
        mockIndexManagerFactory = mock(IndexManagerFactory.class);
        when(openSearchSinkConfiguration.getIndexConfiguration()).thenReturn(indexConfiguration);
        when(indexConfiguration.getIsmPolicyFile()).thenReturn(Optional.empty());
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS);
        when(restHighLevelClient.cluster()).thenReturn(cluster);
        when(cluster.getSettings(any(ClusterGetSettingsRequest.class), any(RequestOptions.class)))
                .thenReturn(clusterGetSettingsResponse);
        when(restHighLevelClient.indices()).thenReturn(indicesClient);
        dynamicIndexManager = new DynamicIndexManager(IndexType.CUSTOM, restHighLevelClient, openSearchSinkConfiguration, mockIndexManagerFactory);
    }

    @Test
    public void dynamicIndexBasicTest() throws IOException {
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS);
        when(clusterGetSettingsResponse.getSetting(IndexConstants.ISM_ENABLED_SETTING)).thenReturn("true");
        String configuredIndexAlias = openSearchSinkConfiguration.getIndexConfiguration().getIndexAlias();
        String expectedIndexAlias = INDEX_ALIAS.replace("${" + ID + "}", DYNAMIC);
        innerIndexManager = mock(IndexManager.class);
        when(mockIndexManagerFactory.getIndexManager(IndexType.CUSTOM, restHighLevelClient, openSearchSinkConfiguration, expectedIndexAlias)).thenReturn(innerIndexManager);
        when(innerIndexManager.getIndexName(expectedIndexAlias)).thenReturn(expectedIndexAlias);
        JacksonEvent event = JacksonEvent.builder().withEventType(EVENT_TYPE).withData(Map.of(ID, DYNAMIC)).build();
        final String indexName = dynamicIndexManager.getIndexName(event.formatString(configuredIndexAlias));
        assertThat(expectedIndexAlias, equalTo(indexName));
    }

    @Test
    public void dynamicIndexWithDateTest() throws IOException {
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS_WITH_DATE_PATTERN);
        when(clusterGetSettingsResponse.getSetting(IndexConstants.ISM_ENABLED_SETTING)).thenReturn("true");
        String configuredIndexAlias = openSearchSinkConfiguration.getIndexConfiguration().getIndexAlias();
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(DATE_PATTERN);
        String expectedIndexAlias = INDEX_ALIAS.replace("${" + ID + "}", DYNAMIC) + "-" + dateFormatter.format(AbstractIndexManager.getCurrentUtcTime());
        innerIndexManager = mock(IndexManager.class);
        when(mockIndexManagerFactory.getIndexManager(IndexType.CUSTOM, restHighLevelClient, openSearchSinkConfiguration, expectedIndexAlias)).thenReturn(innerIndexManager);
        when(innerIndexManager.getIndexName(expectedIndexAlias)).thenReturn(expectedIndexAlias);
        JacksonEvent event = JacksonEvent.builder().withEventType(EVENT_TYPE).withData(Map.of(ID, DYNAMIC)).build();
        final String indexName = dynamicIndexManager.getIndexName(event.formatString(configuredIndexAlias));
        assertThat(expectedIndexAlias, equalTo(indexName));
    }

    @Test
    public void dynamicIndexCacheTest() throws IOException {
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS);
        when(clusterGetSettingsResponse.getSetting(IndexConstants.ISM_ENABLED_SETTING)).thenReturn("true");
        String configuredIndexAlias = openSearchSinkConfiguration.getIndexConfiguration().getIndexAlias();
        String expectedIndexAlias = INDEX_ALIAS.replace("${" + ID + "}", DYNAMIC);
        innerIndexManager = mock(IndexManager.class);
        when(mockIndexManagerFactory.getIndexManager(IndexType.CUSTOM, restHighLevelClient, openSearchSinkConfiguration, expectedIndexAlias)).thenReturn(innerIndexManager);
        when(innerIndexManager.getIndexName(expectedIndexAlias)).thenReturn(expectedIndexAlias);

        JacksonEvent event = JacksonEvent.builder().withEventType(EVENT_TYPE).withData(Map.of(ID, DYNAMIC)).build();
        // Try multiple times to make sure the getIndexManager is not called more than once and cached values are returned
        for (int i = 0; i < 10; i++) {
            final String indexName = dynamicIndexManager.getIndexName(event.formatString(configuredIndexAlias));
            assertThat(expectedIndexAlias, equalTo(indexName));
            verify(mockIndexManagerFactory, times(1)).getIndexManager(eq(IndexType.CUSTOM), eq(restHighLevelClient), eq(openSearchSinkConfiguration), anyString());
        }

        expectedIndexAlias = INDEX_ALIAS.replace("${" + ID + "}", NEW_DYNAMIC);
        when(mockIndexManagerFactory.getIndexManager(IndexType.CUSTOM, restHighLevelClient, openSearchSinkConfiguration, expectedIndexAlias)).thenReturn(innerIndexManager);
        when(innerIndexManager.getIndexName(expectedIndexAlias)).thenReturn(expectedIndexAlias);
        event = JacksonEvent.builder().withEventType(EVENT_TYPE).withData(Map.of(ID, NEW_DYNAMIC)).build();
        final String newIndexName = dynamicIndexManager.getIndexName(event.formatString(configuredIndexAlias));
        // When a new index is used, verify that getIndexManager is called again
        verify(mockIndexManagerFactory, times(2)).getIndexManager(eq(IndexType.CUSTOM), eq(restHighLevelClient), eq(openSearchSinkConfiguration), anyString());
        assertThat(expectedIndexAlias, equalTo(newIndexName));
    }
}
