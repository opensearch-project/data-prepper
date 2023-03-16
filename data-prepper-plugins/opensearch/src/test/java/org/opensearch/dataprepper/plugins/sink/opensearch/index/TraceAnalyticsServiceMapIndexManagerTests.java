/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.opensearch.client.ClusterClient;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexTemplatesResponse;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.cluster.GetClusterSettingsRequest;
import org.opensearch.client.opensearch.cluster.GetClusterSettingsResponse;
import org.opensearch.client.opensearch.cluster.OpenSearchClusterClient;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.opensearch.indices.ExistsTemplateRequest;
import org.opensearch.client.opensearch.indices.GetTemplateRequest;
import org.opensearch.client.opensearch.indices.GetTemplateResponse;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.opensearch.indices.PutTemplateRequest;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.PreSerializedJsonpMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TraceAnalyticsServiceMapIndexManagerTests {
    private static final String INDEX_ALIAS = "trace-service-map-index-alias";
    private static final JsonpMapper JSONP_MAPPER = new PreSerializedJsonpMapper();
    private static final Map<String, JsonData> ISM_ENABLED_SETTING = Map.of(
            "opendistro", JsonData.of(
                    Map.of("index_state_management", Map.of("enabled", true)), JSONP_MAPPER)
    );
    private static final Map<String, JsonData> ISM_DISABLED_SETTING = Map.of(
            "opendistro", JsonData.of(
                    Map.of("index_state_management", Map.of("enabled", false)), JSONP_MAPPER));

    private IndexManagerFactory indexManagerFactory;

    private AbstractIndexManager traceAnalyticsServiceMapIndexManager;

    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    private RestHighLevelClient restHighLevelClient;

    @Mock
    private OpenSearchSinkConfiguration openSearchSinkConfiguration;

    @Mock
    private ClusterClient cluster;

    @Mock
    private OpenSearchClusterClient openSearchClusterClient;

    @Mock
    private ClusterGetSettingsResponse clusterGetSettingsResponse;

    @Mock
    private GetClusterSettingsResponse getClusterSettingsResponse;

    @Mock
    IndexConfiguration indexConfiguration;

    @Mock
    IndicesClient indicesClient;

    @Mock
    private OpenSearchIndicesClient openSearchIndicesClient;

    @Mock
    GetIndexTemplatesResponse getIndexTemplatesResponse;

    @Mock
    GetTemplateResponse getTemplateResponse;

    @Mock
    private OpenSearchTransport openSearchTransport;

    @Before
    public void setup() throws IOException {
        initMocks(this);

        indexManagerFactory = new IndexManagerFactory();
        when(openSearchSinkConfiguration.getIndexConfiguration()).thenReturn(indexConfiguration);
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS);
        traceAnalyticsServiceMapIndexManager
                = indexManagerFactory.getIndexManager(
                        IndexType.TRACE_ANALYTICS_SERVICE_MAP, openSearchClient, restHighLevelClient, openSearchSinkConfiguration);

        when(openSearchClient.cluster()).thenReturn(openSearchClusterClient);
        when(openSearchClusterClient.getSettings(any(GetClusterSettingsRequest.class)))
                .thenReturn(getClusterSettingsResponse);
        when(openSearchClient.indices()).thenReturn(openSearchIndicesClient);
    }

    @Test
    public void constructor_NullRestClient() throws IOException {
        assertThrows(NullPointerException.class, () ->
                indexManagerFactory.getIndexManager(
                        IndexType.TRACE_ANALYTICS_SERVICE_MAP, openSearchClient, null, openSearchSinkConfiguration));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    public void constructor_NullConfiguration() throws IOException {
        assertThrows(NullPointerException.class, () ->
                indexManagerFactory.getIndexManager(
                        IndexType.TRACE_ANALYTICS_SERVICE_MAP, openSearchClient, restHighLevelClient, null));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    public void checkISMEnabled_True() throws IOException {
        when(clusterGetSettingsResponse.getSetting(IndexConstants.ISM_ENABLED_SETTING)).thenReturn("true");
        when(getClusterSettingsResponse.persistent()).thenReturn(ISM_ENABLED_SETTING);
        assertEquals(true, traceAnalyticsServiceMapIndexManager.checkISMEnabled());
        verify(openSearchClient).cluster();
        verify(openSearchClusterClient).getSettings(any(GetClusterSettingsRequest.class));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    public void checkISMEnabled_False() throws IOException {
        when(clusterGetSettingsResponse.getSetting(IndexConstants.ISM_ENABLED_SETTING)).thenReturn("false");
        when(getClusterSettingsResponse.persistent()).thenReturn(ISM_DISABLED_SETTING);
        assertEquals(false, traceAnalyticsServiceMapIndexManager.checkISMEnabled());
        verify(openSearchClient).cluster();
        verify(getClusterSettingsResponse, times(2)).persistent();
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(openSearchClusterClient).getSettings(any(GetClusterSettingsRequest.class));
    }


    @Test
    public void checkAndCreatePolicy() throws IOException {
        assertEquals(Optional.empty(), traceAnalyticsServiceMapIndexManager.checkAndCreatePolicy());
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    public void checkAndCreateIndexTemplate_NoIndexTemplateOnHost_ISMDisabled() throws IOException {
        when(openSearchTransport.jsonpMapper()).thenReturn(new PreSerializedJsonpMapper());
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchIndicesClient.existsTemplate(any(ExistsTemplateRequest.class))).thenReturn(
                new BooleanResponse(false));
        traceAnalyticsServiceMapIndexManager.checkAndCreateIndexTemplate(false, null);
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(openSearchClient, times(2)).indices();
        verify(openSearchIndicesClient).existsTemplate(any(ExistsTemplateRequest.class));
        verify(indexConfiguration).getIndexAlias();
        verify(indexConfiguration).getIndexTemplate();
        verify(openSearchIndicesClient).putTemplate(any(PutTemplateRequest.class));
    }

    @Test
    public void checkAndCreateIndexTemplate_NoIndexTemplateOnHost_ISMEnabled() throws IOException {
        final Map<String, Object> configs = new HashMap<>();
        configs.put("settings", new HashMap<String, String>());
        when(indexConfiguration.getIndexTemplate()).thenReturn(configs);
        when(openSearchTransport.jsonpMapper()).thenReturn(new PreSerializedJsonpMapper());
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchIndicesClient.existsTemplate(any(ExistsTemplateRequest.class))).thenReturn(
                new BooleanResponse(false));
        traceAnalyticsServiceMapIndexManager.checkAndCreateIndexTemplate(true, null);
        verify(openSearchSinkConfiguration, times(3)).getIndexConfiguration();
        verify(openSearchClient, times(2)).indices();
        verify(openSearchIndicesClient).existsTemplate(any(ExistsTemplateRequest.class));
        verify(indexConfiguration).getIndexAlias();
        verify(indexConfiguration, times(3)).getIndexTemplate();
        verify(openSearchIndicesClient).putTemplate(any(PutTemplateRequest.class));
    }

    @Test
    public void checkAndCreateIndexTemplate_ZeroIndexTemplateListInResponse() throws IOException {
        when(openSearchTransport.jsonpMapper()).thenReturn(new PreSerializedJsonpMapper());
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchIndicesClient.existsTemplate(any(ExistsTemplateRequest.class))).thenReturn(
                new BooleanResponse(true));
        when(openSearchIndicesClient.getTemplate(any(GetTemplateRequest.class))).thenReturn(getTemplateResponse);

        try {
            traceAnalyticsServiceMapIndexManager.checkAndCreateIndexTemplate(false, null);
        } catch (final RuntimeException e) {
            verify(openSearchSinkConfiguration).getIndexConfiguration();
            verify(openSearchClient, times(2)).indices();
            verify(openSearchIndicesClient).existsTemplate(any(ExistsTemplateRequest.class));
            verify(indexConfiguration).getIndexAlias();
            verify(openSearchIndicesClient).getTemplate(any(GetTemplateRequest.class));
            verify(getTemplateResponse).result();
        }
    }

    @Test
    public void checkAndCreateIndex_IndexAlreadyExists() throws IOException {
        when(openSearchIndicesClient.exists(any(ExistsRequest.class))).thenReturn(new BooleanResponse(true));
        traceAnalyticsServiceMapIndexManager.checkAndCreateIndex();
        verify(openSearchClient).indices();
        verify(openSearchIndicesClient).exists(any(ExistsRequest.class));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    public void checkAndCreateIndex_NeedToCreateNewIndex() throws IOException {
        when(openSearchIndicesClient.exists(any(ExistsRequest.class))).thenReturn(new BooleanResponse(false));
        when(openSearchIndicesClient.create(any(org.opensearch.client.opensearch.indices.CreateIndexRequest.class)))
                .thenReturn(null);
        traceAnalyticsServiceMapIndexManager.checkAndCreateIndex();
        verify(openSearchClient, times(2)).indices();
        verify(openSearchIndicesClient).exists(any(ExistsRequest.class));
        verify(openSearchIndicesClient).create(any(org.opensearch.client.opensearch.indices.CreateIndexRequest.class));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    public void checkAndCreateIndex_CreateNewIndex_Exception() throws IOException {
        when(openSearchIndicesClient.exists(any(ExistsRequest.class))).thenReturn(
                new BooleanResponse(false));
        when(openSearchIndicesClient.create(any(org.opensearch.client.opensearch.indices.CreateIndexRequest.class))).thenThrow(new OpenSearchException(""));
        try {
            traceAnalyticsServiceMapIndexManager.checkAndCreateIndex();
        } catch (final IOException e) {
            verify(openSearchClient, times(2)).indices();
            verify(openSearchIndicesClient).exists(any(ExistsRequest.class));
            verify(openSearchIndicesClient).create(any(org.opensearch.client.opensearch.indices.CreateIndexRequest.class));
            verify(openSearchSinkConfiguration).getIndexConfiguration();
            verify(indexConfiguration).getIndexAlias();
        }
    }

    @After
    public void clear() {
        verifyNoMoreInteractions(
                restHighLevelClient,
                openSearchSinkConfiguration,
                cluster,
                clusterGetSettingsResponse,
                indexConfiguration,
                indicesClient,
                getIndexTemplatesResponse
        );
    }
}
