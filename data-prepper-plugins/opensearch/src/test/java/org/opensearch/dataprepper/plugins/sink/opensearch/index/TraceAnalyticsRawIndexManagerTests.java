/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexTemplatesResponse;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.cluster.ExistsComponentTemplateRequest;
import org.opensearch.client.opensearch.cluster.GetClusterSettingsRequest;
import org.opensearch.client.opensearch.cluster.GetClusterSettingsResponse;
import org.opensearch.client.opensearch.cluster.OpenSearchClusterClient;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsAliasRequest;
import org.opensearch.client.opensearch.indices.GetTemplateResponse;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.PreSerializedJsonpMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TraceAnalyticsRawIndexManagerTests {
    private static final String INDEX_ALIAS = "trace-raw-index-alias";
    private static final String EXPECTED_TEMPLATE_NAME = INDEX_ALIAS + "-index-template";

    private IndexManagerFactory indexManagerFactory;

    private AbstractIndexManager traceAnalyticsRawIndexManager;

    private AbstractIndexManager traceAnalyticsRawStandardIndexManager;

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
    private IndexConfiguration indexConfiguration;

    @Mock
    private IndicesClient indicesClient;

    @Mock
    private OpenSearchIndicesClient openSearchIndicesClient;

    @Mock
    private GetIndexTemplatesResponse getIndexTemplatesResponse;

    @Mock
    private GetTemplateResponse getTemplateResponse;

    @Mock
    private RestClient restClient;

    @Mock
    private ResponseException responseException;

    @Mock
    private BooleanResponse booleanResponse;

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
        traceAnalyticsRawIndexManager = indexManagerFactory.getIndexManager(
                IndexType.TRACE_ANALYTICS_RAW, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
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
    void constructor_NullRestClient() {
        assertThrows(NullPointerException.class, () ->
                indexManagerFactory.getIndexManager(
                        IndexType.TRACE_ANALYTICS_RAW, openSearchClient, null, openSearchSinkConfiguration, templateStrategy));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void constructor_NullConfiguration() {
        assertThrows(NullPointerException.class, () ->
                indexManagerFactory.getIndexManager(
                        IndexType.TRACE_ANALYTICS_RAW, openSearchClient, restHighLevelClient, null, templateStrategy));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void checkISMEnabled_True() throws IOException {
        when(clusterSettingsParser.getStringValueClusterSetting(any(GetClusterSettingsResponse.class), anyString())).thenReturn("true");
        assertEquals(true, traceAnalyticsRawIndexManager.checkISMEnabled());
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(openSearchClient).cluster();
        verify(openSearchClusterClient).getSettings(any(GetClusterSettingsRequest.class));
    }

    @Test
    void checkISMEnabled_False() throws IOException {
        when(clusterSettingsParser.getStringValueClusterSetting(any(GetClusterSettingsResponse.class), anyString())).thenReturn("false");
        assertEquals(false, traceAnalyticsRawIndexManager.checkISMEnabled());
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(openSearchClient).cluster();
        verify(openSearchClusterClient).getSettings(any(GetClusterSettingsRequest.class));
    }

    @Test
    void checkAndCreatePolicy_Normal() throws IOException {
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        assertEquals(Optional.empty(), traceAnalyticsRawIndexManager.checkAndCreatePolicy());
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(restHighLevelClient).getLowLevelClient();
        verify(restClient).performRequest(any());
    }

    @Test
    void checkAndCreatePolicy_ExceptionFirstThenSucceeds() throws IOException {
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        when(restClient.performRequest(any())).thenThrow(responseException).thenReturn(null);
        when(responseException.getMessage()).thenReturn("Invalid field: [ism_template]");
        assertEquals(Optional.of("raw-span-policy"), traceAnalyticsRawIndexManager.checkAndCreatePolicy());
        verify(restHighLevelClient, times(2)).getLowLevelClient();
        verify(restClient, times(2)).performRequest(any());
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void checkAndCreatePolicy_withPlainV2IndexManager_usesV2Policy() throws IOException {
        final AbstractIndexManager traceAnalyticsRawPlainV2IndexManager = indexManagerFactory.getIndexManager(
                IndexType.TRACE_ANALYTICS_RAW_PLAIN_V2, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        when(restClient.performRequest(any())).thenThrow(responseException).thenReturn(null);
        when(responseException.getMessage()).thenReturn("Invalid field: [ism_template]");
        assertEquals(Optional.of("otel-v2-apm-span-policy"), traceAnalyticsRawPlainV2IndexManager.checkAndCreatePolicy());
        verify(restClient, times(2)).performRequest(any());
        verify(restHighLevelClient, times(2)).getLowLevelClient();
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration, times(2)).getIndexAlias();
    }

    @Test
    void checkAndCreateComponentTemplates_withPlainV2IndexManager_createsMissingAndOlderComponentTemplates() throws IOException {
        final AbstractIndexManager traceAnalyticsRawPlainV2IndexManager = indexManagerFactory.getIndexManager(
                IndexType.TRACE_ANALYTICS_RAW_PLAIN_V2, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        final Map<String, Object> missing = Map.of("version", 1, "template", Map.of());
        final Map<String, Object> current = Map.of("version", 2, "template", Map.of());
        final Map<String, Object> older = Map.of("version", 3, "template", Map.of());
        final Map<String, Map<String, Object>> componentTemplates = new LinkedHashMap<>();
        componentTemplates.put("missing", missing);
        componentTemplates.put("current", current);
        componentTemplates.put("older", older);
        when(indexConfiguration.getManagedComponentTemplates()).thenReturn(componentTemplates);
        stubExistingComponentTemplates(Map.of("current", 2L, "older", 1L));

        traceAnalyticsRawPlainV2IndexManager.checkAndCreateComponentTemplates();

        verify(openSearchTransport).performRequest(eq(missing), any(), any());
        verify(openSearchTransport, never()).performRequest(eq(current), any(), any());
        verify(openSearchTransport).performRequest(eq(older), any(), any());
        verify(openSearchSinkConfiguration, times(3)).getIndexConfiguration();
        verify(indexConfiguration, times(2)).getIndexAlias();
        verify(indexConfiguration).getManagedComponentTemplates();
    }

    @Test
    void setupIndex_withPlainV2IndexManager_createsComponentTemplatesBeforeIndexTemplate() throws IOException {
        final AbstractIndexManager traceAnalyticsRawPlainV2IndexManager = indexManagerFactory.getIndexManager(
                IndexType.TRACE_ANALYTICS_RAW_PLAIN_V2, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        final Map<String, Object> componentTemplate = Map.of("version", 1, "template", Map.of());
        when(indexConfiguration.getManagedComponentTemplates()).thenReturn(Map.of("component", componentTemplate));
        stubExistingComponentTemplates(Map.of());
        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME)).thenReturn(Optional.empty());
        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(true));

        traceAnalyticsRawPlainV2IndexManager.setupIndex();

        final InOrder inOrder = inOrder(openSearchTransport, templateStrategy);
        inOrder.verify(openSearchTransport).performRequest(eq(componentTemplate), any(), any());
        inOrder.verify(templateStrategy).createTemplate(indexTemplateObject);
        verify(openSearchSinkConfiguration, atLeastOnce()).getIndexConfiguration();
        verify(indexConfiguration, atLeastOnce()).getIndexAlias();
        verify(indexConfiguration, atLeastOnce()).getIndexTemplate();
        verify(indexConfiguration).getManagedComponentTemplates();
    }

    private void stubExistingComponentTemplates(final Map<String, Long> existingVersions) throws IOException {
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchClusterClient.existsComponentTemplate(any(ExistsComponentTemplateRequest.class)))
                .thenAnswer(invocation -> new BooleanResponse(existingVersions.containsKey(
                        invocation.<ExistsComponentTemplateRequest>getArgument(0).name().get(0))));
        when(openSearchTransport.performRequest(isNull(), any(), any()))
                .thenAnswer(invocation -> {
                    final String path = invocation.<Endpoint<?, ?, ?>>getArgument(1).requestUrl(null);
                    final String name = path.substring(path.lastIndexOf('/') + 1);
                    return ComponentTemplateAPIWrapperTest.toJsonData("{\"component_templates\":[{\"name\":\"" + name
                            + "\",\"component_template\":{\"template\":{},\"version\":" + existingVersions.get(name) + "}}]}");
                });
    }

    @Test
    void checkAndCreateIndexTemplate_NoIndexTemplateOnHost_ISMDisabled() throws IOException {
        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME))
                .thenReturn(Optional.empty());

        traceAnalyticsRawIndexManager.checkAndCreateIndexTemplate(false, null);

        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(indexConfiguration).getIndexTemplate();
        verify(templateStrategy).getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME);
        verify(templateStrategy).createTemplate(indexTemplateObject);
        verify(indexTemplateObject).setTemplateName(EXPECTED_TEMPLATE_NAME);
        verify(indexTemplateObject).setIndexPatterns(Collections.singletonList(INDEX_ALIAS + "-*"));
        verify(indexTemplateObject, never()).putCustomSetting(eq(IndexConstants.ISM_ROLLOVER_ALIAS_SETTING), anyString());
        verify(indexTemplateObject, never()).putCustomSetting(eq(IndexConstants.ISM_POLICY_ID_SETTING), anyString());
    }

    @Test
    void checkAndCreateIndexTemplate_NoIndexTemplateOnHost_ISMEnabled() throws IOException {
        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME))
                .thenReturn(Optional.empty());
        traceAnalyticsRawIndexManager.checkAndCreateIndexTemplate(true, null);

        verify(openSearchSinkConfiguration, atLeastOnce()).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(indexConfiguration, atLeastOnce()).getIndexTemplate();
        verify(indexTemplateObject).setTemplateName(EXPECTED_TEMPLATE_NAME);
        verify(indexTemplateObject).setIndexPatterns(Collections.singletonList(INDEX_ALIAS + "-*"));
        verify(indexTemplateObject).putCustomSetting(IndexConstants.ISM_ROLLOVER_ALIAS_SETTING, INDEX_ALIAS);
        verify(indexTemplateObject, never()).putCustomSetting(eq(IndexConstants.ISM_POLICY_ID_SETTING), anyString());
        verify(templateStrategy).getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME);
        verify(templateStrategy).createTemplate(indexTemplateObject);
    }

    @Test
    void checkAndCreateIndexTemplate_ZeroIndexTemplateListInResponse() throws IOException {
        when(booleanResponse.value()).thenReturn(true);
        when(openSearchTransport.jsonpMapper()).thenReturn(new PreSerializedJsonpMapper());
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME))
                .thenReturn(Optional.of(0L));

        traceAnalyticsRawIndexManager.checkAndCreateIndexTemplate(false, null);
        verify(openSearchSinkConfiguration, atLeastOnce()).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(indexConfiguration).getIndexTemplate();
        verify(templateStrategy).getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME);
        verify(templateStrategy, never()).createTemplate(any());
    }

    @Test
    void checkAndCreateIndex_IndexAlreadyExists() throws IOException {
        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(true));
        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME))
                .thenReturn(Optional.of(0L));
        traceAnalyticsRawIndexManager.checkAndCreateIndex();
        verify(openSearchClient).indices();
        verify(openSearchIndicesClient).existsAlias(any(ExistsAliasRequest.class));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(templateStrategy, never()).createTemplate(any());
    }

    @Test
    void checkAndCreateIndex_NeedToCreateNewIndex() throws IOException {
        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(false));
        when(openSearchIndicesClient.create(any(CreateIndexRequest.class)))
                .thenReturn(null);
        traceAnalyticsRawIndexManager.checkAndCreateIndex();
        verify(openSearchClient, times(2)).indices();
        verify(openSearchIndicesClient).existsAlias(any(ExistsAliasRequest.class));
        verify(openSearchIndicesClient).create(any(CreateIndexRequest.class));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void checkAndCreateIndex_NeedToCreateNewIndex_withStandardIndexManager() throws IOException {
        traceAnalyticsRawStandardIndexManager = indexManagerFactory.getIndexManager(
                IndexType.TRACE_ANALYTICS_RAW_PLAIN, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);

        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(false));
        when(openSearchIndicesClient.create(any(CreateIndexRequest.class)))
                .thenReturn(null);
        traceAnalyticsRawStandardIndexManager.checkAndCreateIndex();
        verify(openSearchClient, times(2)).indices();
        verify(openSearchIndicesClient).existsAlias(any(ExistsAliasRequest.class));
        verify(openSearchIndicesClient).create(any(CreateIndexRequest.class));
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration, times(2)).getIndexAlias();
    }

    @Test
    void checkAndCreateIndex_NeedToCreateNewIndex_withPlainV2IndexManager() throws IOException {
        final AbstractIndexManager traceAnalyticsRawPlainV2IndexManager = indexManagerFactory.getIndexManager(
                IndexType.TRACE_ANALYTICS_RAW_PLAIN_V2, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);

        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(false));
        when(openSearchIndicesClient.create(any(CreateIndexRequest.class)))
                .thenReturn(null);
        traceAnalyticsRawPlainV2IndexManager.checkAndCreateIndex();
        verify(openSearchClient, times(2)).indices();
        verify(openSearchIndicesClient).existsAlias(any(ExistsAliasRequest.class));
        verify(openSearchIndicesClient).create(any(CreateIndexRequest.class));
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration, times(2)).getIndexAlias();
    }

    @Test
    void checkAndCreateIndex_CreateNewIndex_Exception() throws IOException {
        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(
                new BooleanResponse(false));
        when(openSearchException.getMessage()).thenReturn("");
        when(openSearchIndicesClient.create(any(CreateIndexRequest.class)))
                .thenThrow(openSearchException);

        assertThrows(IOException.class, () -> traceAnalyticsRawIndexManager.checkAndCreateIndex());

        verify(openSearchClient, times(2)).indices();
        verify(openSearchIndicesClient).existsAlias(any(ExistsAliasRequest.class));
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
                getIndexTemplatesResponse,
                restClient
        );
    }
}
