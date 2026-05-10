/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexTemplatesResponse;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.cluster.GetClusterSettingsRequest;
import org.opensearch.client.opensearch.cluster.GetClusterSettingsResponse;
import org.opensearch.client.opensearch.cluster.OpenSearchClusterClient;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.opensearch.indices.GetTemplateRequest;
import org.opensearch.client.opensearch.indices.GetTemplateResponse;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TraceAnalyticsServiceMapIndexManagerTests {
    private static final String INDEX_ALIAS = "trace-service-map-index-alias";
    private static final String EXPECTED_TEMPLATE_NAME = INDEX_ALIAS + "-index-template";


    private IndexManagerFactory indexManagerFactory;

    private AbstractIndexManager traceAnalyticsServiceMapIndexManager;

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
    private OpenSearchClusterClient openSearchClusterClient;

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

    @Mock
    private OpenSearchException openSearchException;

    private Map<String, Object> indexTemplateMap;
    @Mock
    private IndexTemplate indexTemplateObject;

    @BeforeEach
    void setup() throws IOException {
        initMocks(this);

        indexManagerFactory = new IndexManagerFactory(clusterSettingsParser);
        when(openSearchSinkConfiguration.getIndexConfiguration()).thenReturn(indexConfiguration);
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS);
        traceAnalyticsServiceMapIndexManager
                = indexManagerFactory.getIndexManager(
                        IndexType.TRACE_ANALYTICS_SERVICE_MAP, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);

        when(openSearchClient.cluster()).thenReturn(openSearchClusterClient);
        when(openSearchClusterClient.getSettings(any(GetClusterSettingsRequest.class)))
                .thenReturn(getClusterSettingsResponse);
        when(openSearchClient.indices()).thenReturn(openSearchIndicesClient);

        indexTemplateMap = Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        when(indexConfiguration.getIndexTemplate())
                .thenReturn(indexTemplateMap);
        when(templateStrategy.createIndexTemplate(indexTemplateMap))
                .thenReturn(indexTemplateObject);
    }

    @Test
    void constructor_NullRestClient() throws IOException {
        assertThrows(NullPointerException.class, () ->
                indexManagerFactory.getIndexManager(
                        IndexType.TRACE_ANALYTICS_SERVICE_MAP, openSearchClient, null, openSearchSinkConfiguration, templateStrategy));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void constructor_NullConfiguration() throws IOException {
        assertThrows(NullPointerException.class, () ->
                indexManagerFactory.getIndexManager(
                        IndexType.TRACE_ANALYTICS_SERVICE_MAP, openSearchClient, restHighLevelClient, null, templateStrategy));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void checkISMEnabled_True() throws IOException {
        when(clusterSettingsParser.getStringValueClusterSetting(any(GetClusterSettingsResponse.class), anyString())).thenReturn("true");
        assertEquals(true, traceAnalyticsServiceMapIndexManager.checkISMEnabled());
        verify(openSearchClient).cluster();
        verify(openSearchClusterClient).getSettings(any(GetClusterSettingsRequest.class));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void checkISMEnabledByDefault_False() throws IOException {
        when(clusterSettingsParser.getStringValueClusterSetting(any(GetClusterSettingsResponse.class), anyString())).thenReturn("false");
        assertEquals(false, traceAnalyticsServiceMapIndexManager.checkISMEnabled());
        verify(openSearchClient).cluster();
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(openSearchClusterClient).getSettings(any(GetClusterSettingsRequest.class));
    }

    @Test
    void checkAndCreatePolicy() throws IOException {
        assertEquals(Optional.empty(), traceAnalyticsServiceMapIndexManager.checkAndCreatePolicy(INDEX_ALIAS));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void checkAndCreateIndexTemplate_NoIndexTemplateOnHost_ISMDisabled() throws IOException {
        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME))
                .thenReturn(Optional.empty());

        traceAnalyticsServiceMapIndexManager.checkAndCreateIndexTemplate(false, null);

        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(indexConfiguration).getIndexTemplate();
        verify(templateStrategy).getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME);
        verify(templateStrategy).createTemplate(indexTemplateObject);
        verify(indexTemplateObject).setTemplateName(EXPECTED_TEMPLATE_NAME);
        verify(indexTemplateObject).setIndexPatterns(Collections.singletonList(INDEX_ALIAS));
        verify(indexTemplateObject, never()).putCustomSetting(eq(IndexConstants.ISM_ROLLOVER_ALIAS_SETTING), anyString());
        verify(indexTemplateObject, never()).putCustomSetting(eq(IndexConstants.ISM_POLICY_ID_SETTING), anyString());
    }

    @Test
    void checkAndCreateIndexTemplate_NoIndexTemplateOnHost_ISMEnabled() throws IOException {
        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME))
                .thenReturn(Optional.empty());

        traceAnalyticsServiceMapIndexManager.checkAndCreateIndexTemplate(true, null);
        verify(openSearchSinkConfiguration, atLeastOnce()).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(indexConfiguration, atLeastOnce()).getIndexTemplate();
        verify(indexTemplateObject).setTemplateName(EXPECTED_TEMPLATE_NAME);
        verify(indexTemplateObject).setIndexPatterns(Collections.singletonList(INDEX_ALIAS));
        verify(indexTemplateObject).putCustomSetting(IndexConstants.ISM_ROLLOVER_ALIAS_SETTING, INDEX_ALIAS);
        verify(indexTemplateObject, never()).putCustomSetting(eq(IndexConstants.ISM_POLICY_ID_SETTING), anyString());
        verify(templateStrategy).getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME);
        verify(templateStrategy).createTemplate(indexTemplateObject);
    }

    @Test
    void checkAndCreateIndexTemplate_ZeroIndexTemplateListInResponse() throws IOException {
        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME))
                .thenReturn(Optional.of(0L));
        when(openSearchIndicesClient.getTemplate(any(GetTemplateRequest.class))).thenReturn(getTemplateResponse);

        traceAnalyticsServiceMapIndexManager.checkAndCreateIndexTemplate(false, null);

        verify(openSearchSinkConfiguration, atLeastOnce()).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(indexConfiguration).getIndexTemplate();
        verify(templateStrategy).getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME);
        verify(templateStrategy, never()).createTemplate(any());
    }

    @Test
    void checkAndCreateIndex_IndexAlreadyExists() throws IOException {
        when(openSearchIndicesClient.exists(any(ExistsRequest.class))).thenReturn(new BooleanResponse(true));
        traceAnalyticsServiceMapIndexManager.checkAndCreateIndex();
        verify(openSearchClient).indices();
        verify(openSearchIndicesClient).exists(any(ExistsRequest.class));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void checkAndCreateIndex_NeedToCreateNewIndex() throws IOException {
        when(openSearchIndicesClient.exists(any(ExistsRequest.class))).thenReturn(new BooleanResponse(false));
        when(openSearchIndicesClient.create(any(CreateIndexRequest.class)))
                .thenReturn(null);
        traceAnalyticsServiceMapIndexManager.checkAndCreateIndex();
        verify(openSearchClient, times(2)).indices();
        verify(openSearchIndicesClient).exists(any(ExistsRequest.class));
        verify(openSearchIndicesClient).create(any(CreateIndexRequest.class));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void checkAndCreateIndex_CreateNewIndex_Exception() throws IOException {
        when(openSearchIndicesClient.exists(any(ExistsRequest.class))).thenReturn(
                new BooleanResponse(false));
        when(openSearchException.getMessage()).thenReturn("");
        when(openSearchIndicesClient.create(any(CreateIndexRequest.class)))
                .thenThrow(openSearchException);

        assertThrows(IOException.class, () -> traceAnalyticsServiceMapIndexManager.checkAndCreateIndex());
        verify(openSearchClient, times(2)).indices();
        verify(openSearchIndicesClient).exists(any(ExistsRequest.class));
        verify(openSearchIndicesClient).create(any(CreateIndexRequest.class));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @AfterEach
    void clear() {
        verifyNoMoreInteractions(
                restHighLevelClient,
                openSearchSinkConfiguration,
                indexConfiguration,
                indicesClient,
                getIndexTemplatesResponse
        );
    }
}
