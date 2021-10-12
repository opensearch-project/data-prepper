package com.amazon.dataprepper.plugins.sink.opensearch.index;

import com.amazon.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TraceAnalyticsRawIndexManagerTests {
    private static final String INDEX_ALIAS = "trace-raw-index-alias";

    private IndexManagerFactory indexManagerFactory;

    private IndexManager traceAnalyticsRawIndexManager;

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

    @Before
    public void setup() throws IOException {
        initMocks(this);

        indexManagerFactory = new IndexManagerFactory();

        traceAnalyticsRawIndexManager = indexManagerFactory.getIndexManager(IndexType.TRACE_ANALYTICS_RAW,
                restHighLevelClient, openSearchSinkConfiguration);

        when(restHighLevelClient.cluster()).thenReturn(cluster);
        when(cluster.getSettings(any(ClusterGetSettingsRequest.class), any(RequestOptions.class)))
                .thenReturn(clusterGetSettingsResponse);

        when(openSearchSinkConfiguration.getIndexConfiguration()).thenReturn(indexConfiguration);
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS);
        when(restHighLevelClient.indices()).thenReturn(indicesClient);
    }

    @Test
    public void constructor_NullRestClient() {
        assertThrows(NullPointerException.class, () ->
                indexManagerFactory.getIndexManager(IndexType.TRACE_ANALYTICS_RAW,null, openSearchSinkConfiguration));
    }

    @Test
    public void constructor_NullConfiguration() {
        assertThrows(NullPointerException.class, () ->
                indexManagerFactory.getIndexManager(IndexType.TRACE_ANALYTICS_RAW, restHighLevelClient, null));
    }

    @Test
    public void checkISMEnabled_True() throws IOException {
        when(clusterGetSettingsResponse.getSetting(IndexConstants.ISM_ENABLED_SETTING)).thenReturn("true");
        assertEquals(true, traceAnalyticsRawIndexManager.checkISMEnabled());
        verify(restHighLevelClient).cluster();
        verify(cluster).getSettings(any(), any());
        verify(clusterGetSettingsResponse).getSetting(any());
    }

    @Test
    public void checkISMEnabled_False() throws IOException {
        when(clusterGetSettingsResponse.getSetting(IndexConstants.ISM_ENABLED_SETTING)).thenReturn("false");
        assertEquals(false, traceAnalyticsRawIndexManager.checkISMEnabled());
        verify(restHighLevelClient).cluster();
        verify(cluster).getSettings(any(), any());
        verify(clusterGetSettingsResponse).getSetting(any());
    }

    @Test
    public void checkAndCreatePolicy_Normal() throws IOException {
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        assertEquals(Optional.empty(), traceAnalyticsRawIndexManager.checkAndCreatePolicy());
        verify(restHighLevelClient).getLowLevelClient();
        verify(restClient).performRequest(any());
    }

    @Test
    public void checkAndCreatePolicy_ExceptionFirstThenSucceed() throws IOException {
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        when(restClient.performRequest(any())).thenThrow(responseException).thenReturn(null);
        when(responseException.getMessage()).thenReturn("Invalid field: [ism_template]");
        assertEquals(Optional.of("raw-span-policy"), traceAnalyticsRawIndexManager.checkAndCreatePolicy());
        verify(restHighLevelClient, times(2)).getLowLevelClient();
        verify(restClient, times(2)).performRequest(any());
    }

    @Test
    public void checkAndCreateIndexTemplate_NoIndexTemplateOnHost_ISMDisabled() throws IOException {
        when(indicesClient.existsTemplate(any(), any())).thenReturn(false);
        traceAnalyticsRawIndexManager.checkAndCreateIndexTemplate(false, null);
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(restHighLevelClient, times(2)).indices();
        verify(indicesClient).existsTemplate(any(), any());
        verify(indexConfiguration).getIndexAlias();
        verify(indexConfiguration).getIndexTemplate();
        verify(indicesClient).putTemplate(any(PutIndexTemplateRequest.class), any());
    }

    @Test
    public void checkAndCreateIndexTemplate_NoIndexTemplateOnHost_ISMEnabled() throws IOException {
        final Map<String, Object> configs = new HashMap<>();
        configs.put("settings", new HashMap<String, String>());
        when(indexConfiguration.getIndexTemplate()).thenReturn(configs);
        when(indicesClient.existsTemplate(any(), any())).thenReturn(false);
        traceAnalyticsRawIndexManager.checkAndCreateIndexTemplate(true, null);
        verify(openSearchSinkConfiguration, times(3)).getIndexConfiguration();
        verify(restHighLevelClient, times(2)).indices();
        verify(indicesClient).existsTemplate(any(), any());
        verify(indexConfiguration).getIndexAlias();
        verify(indexConfiguration, times(3)).getIndexTemplate();
        verify(indicesClient).putTemplate(any(PutIndexTemplateRequest.class), any());
    }

    @Test
    public void checkAndCreateIndexTemplate_ZeroIndexTemplateListInResponse() throws IOException {
        when(indicesClient.existsTemplate(any(), any())).thenReturn(true);
        when(indicesClient.getIndexTemplate(any(GetIndexTemplatesRequest.class), any())).thenReturn(getIndexTemplatesResponse);

        try {
            traceAnalyticsRawIndexManager.checkAndCreateIndexTemplate(false, null);
        } catch (final RuntimeException e) {
            verify(openSearchSinkConfiguration).getIndexConfiguration();
            verify(restHighLevelClient, times(2)).indices();
            verify(indicesClient).existsTemplate(any(), any());
            verify(indexConfiguration).getIndexAlias();
            verify(indicesClient).existsTemplate(any(), any());
            verify(indicesClient).getIndexTemplate(any(GetIndexTemplatesRequest.class), any());
            verify(getIndexTemplatesResponse, times(2)).getIndexTemplates();
        }
    }

    @Test
    public void checkAndCreateIndex_IndexAlreadyExists() throws IOException {
        when(indicesClient.existsAlias(any(GetAliasesRequest.class), any())).thenReturn(true);
        traceAnalyticsRawIndexManager.checkAndCreateIndex();
        verify(restHighLevelClient).indices();
        verify(indicesClient).existsAlias(any(GetAliasesRequest.class), any());
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    public void checkAndCreateIndex_NeedToCreateNewIndex() throws IOException {
        when(indicesClient.existsAlias(any(GetAliasesRequest.class), any())).thenReturn(false);
        when(indicesClient.create(any(CreateIndexRequest.class), any())).thenReturn(null);
        traceAnalyticsRawIndexManager.checkAndCreateIndex();
        verify(restHighLevelClient, times(2)).indices();
        verify(indicesClient).existsAlias(any(GetAliasesRequest.class), any());
        verify(indicesClient).create(any(CreateIndexRequest.class), any());
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    public void checkAndCreateIndex_CreateNewIndex_Exception() throws IOException {
        when(indicesClient.exists(any(GetIndexRequest.class), any())).thenReturn(false);
        when(indicesClient.create(any(CreateIndexRequest.class), any())).thenThrow(new OpenSearchException(""));
        try {
            traceAnalyticsRawIndexManager.checkAndCreateIndex();
        } catch (final IOException e) {
            verify(restHighLevelClient, times(2)).indices();
            verify(indicesClient).existsAlias(any(GetAliasesRequest.class), any());
            verify(indicesClient).create(any(CreateIndexRequest.class), any());
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
                getIndexTemplatesResponse,
                restClient
        );
    }
}
