/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.opensearch.client.ClusterClient;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexTemplatesRequest;
import org.opensearch.client.indices.GetIndexTemplatesResponse;
import org.opensearch.client.indices.PutIndexTemplateRequest;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class DynamicIndexManagerTests {

    private static final String ID = "id";
    private static final String DYNAMIC = "dynamic";
    private static final String INDEX_ALIAS = "test-${" + ID + "}-index-alias";
    private static final String DATE_PATTERN = "yyyy.MM.dd";
    private static final String INDEX_ALIAS_WITH_DATE_PATTERN = INDEX_ALIAS+ "-%{" + DATE_PATTERN + "}";

    private IndexManagerFactory indexManagerFactory;
    private IndexManager indexManager;

    private DynamicIndexManager dynamicIndexManager;

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

    @Before
    public void setup() throws IOException {
        initMocks(this);

        indexManagerFactory = new IndexManagerFactory();
        when(openSearchSinkConfiguration.getIndexConfiguration()).thenReturn(indexConfiguration);
        when(indexConfiguration.getIsmPolicyFile()).thenReturn(Optional.empty());
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS);
        when(restHighLevelClient.cluster()).thenReturn(cluster);
        when(cluster.getSettings(any(ClusterGetSettingsRequest.class), any(RequestOptions.class)))
                .thenReturn(clusterGetSettingsResponse);
        when(restHighLevelClient.indices()).thenReturn(indicesClient);
    }

    @Test
    public void dynamicIndexBasicTest(){
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS);
        when(clusterGetSettingsResponse.getSetting(IndexConstants.ISM_ENABLED_SETTING)).thenReturn("true");
        String configuredIndexAlias = openSearchSinkConfiguration.getIndexConfiguration().getIndexAlias();
	try {
            indexManager = indexManagerFactory.getIndexManager(IndexType.CUSTOM, restHighLevelClient, openSearchSinkConfiguration, configuredIndexAlias);
	    assertTrue(indexManager instanceof DynamicIndexManager);
	    JacksonEvent event = JacksonEvent.builder().withEventType(EVENT_TYPE).withData(Map.of(ID, DYNAMIC)).build();
	    final String indexName = indexManager.getIndexName(event.formatString(configuredIndexAlias));
	    String expectedIndexPattern = INDEX_ALIAS.replace("${" + ID + "}", DYNAMIC);
	    assertTrue(expectedIndexPattern.equals(indexName));
	} catch (IOException e) {}
    }

    @Test
    public void dynamicIndexWithDateTest(){
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS_WITH_DATE_PATTERN);
        when(clusterGetSettingsResponse.getSetting(IndexConstants.ISM_ENABLED_SETTING)).thenReturn("true");
        String configuredIndexAlias = openSearchSinkConfiguration.getIndexConfiguration().getIndexAlias();
	try {
            indexManager = indexManagerFactory.getIndexManager(IndexType.CUSTOM, restHighLevelClient, openSearchSinkConfiguration, configuredIndexAlias);
	    assertTrue(indexManager instanceof DynamicIndexManager);
	    JacksonEvent event = JacksonEvent.builder().withEventType(EVENT_TYPE).withData(Map.of(ID, DYNAMIC)).build();
	    final String indexName = indexManager.getIndexName(event.formatString(configuredIndexAlias));
	    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(DATE_PATTERN);
	    String expectedIndexPattern = INDEX_ALIAS.replace("${" + ID + "}", DYNAMIC) + "-" + dateFormatter.format(AbstractIndexManager.getCurrentUtcTime());
	    assertTrue(expectedIndexPattern.equals(indexName));
	} catch (IOException e) {}
    }
}
