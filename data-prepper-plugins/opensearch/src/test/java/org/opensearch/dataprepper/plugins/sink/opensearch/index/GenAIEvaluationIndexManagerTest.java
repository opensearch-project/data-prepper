/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.cluster.ExistsComponentTemplateRequest;
import org.opensearch.client.opensearch.cluster.GetClusterSettingsRequest;
import org.opensearch.client.opensearch.cluster.GetClusterSettingsResponse;
import org.opensearch.client.opensearch.cluster.OpenSearchClusterClient;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsAliasRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class GenAIEvaluationIndexManagerTest {
    private static final String INDEX_ALIAS = IndexConstants.GENAI_EVALUATION_DEFAULT_ALIAS;
    private static final String EXPECTED_TEMPLATE_NAME = INDEX_ALIAS + "-index-template";
    private static final String EXPECTED_POLICY_NAME = "otel-v1-genai-evaluation-policy";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private IndexManagerFactory indexManagerFactory;

    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    private RestHighLevelClient restHighLevelClient;

    @Mock
    private RestClient restClient;

    @Mock
    private ResponseException responseException;

    @Mock
    private OpenSearchSinkConfiguration openSearchSinkConfiguration;

    @Mock
    private IndexConfiguration indexConfiguration;

    @Mock
    private ClusterSettingsParser clusterSettingsParser;

    @Mock
    private TemplateStrategy templateStrategy;

    @Mock
    private IndexTemplate indexTemplate;

    @Mock
    private OpenSearchIndicesClient openSearchIndicesClient;

    @Mock
    private OpenSearchClusterClient openSearchClusterClient;

    @Mock
    private OpenSearchTransport openSearchTransport;

    @Mock
    private GetClusterSettingsResponse getClusterSettingsResponse;

    @Captor
    private ArgumentCaptor<Endpoint<Map<String, Object>, Object, Object>> endpointCaptor;

    @BeforeEach
    void setup() {
        indexManagerFactory = new IndexManagerFactory(clusterSettingsParser);
        when(openSearchSinkConfiguration.getIndexConfiguration()).thenReturn(indexConfiguration);
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS);
    }

    private void stubIndexTemplate() {
        final Map<String, Object> indexTemplateMap = Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        when(indexConfiguration.getIndexTemplate()).thenReturn(indexTemplateMap);
        when(templateStrategy.createIndexTemplate(indexTemplateMap)).thenReturn(indexTemplate);
    }

    private AbstractIndexManager createObjectUnderTest(final IndexType indexType) {
        return indexManagerFactory.getIndexManager(
                indexType, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
    }

    @Test
    void checkAndCreatePolicy_putsTheGenAIEvaluationPolicyWithIsmTemplate() throws IOException {
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        final AbstractIndexManager objectUnderTest = createObjectUnderTest(IndexType.GENAI_EVALUATION_PLAIN);

        final Optional<String> policyId = objectUnderTest.checkAndCreatePolicy();

        assertThat(policyId, equalTo(Optional.empty()));
        final ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
        verify(restClient).performRequest(requestCaptor.capture());
        final Request request = requestCaptor.getValue();
        assertThat(request.getMethod(), equalTo("PUT"));
        assertThat(request.getEndpoint(), equalTo("/_opendistro/_ism/policies/" + EXPECTED_POLICY_NAME));
        final Map<String, Object> policy = readPolicy(request);
        assertThat(policy.get("default_state"), equalTo("current_write_index"));
        assertThat(policy.get("ism_template"), equalTo(Map.of("index_patterns", List.of(INDEX_ALIAS + "-*"))));
        assertThat(policy.get("states"), equalTo(List.of(Map.of(
                "name", "current_write_index",
                "actions", List.of(Map.of("rollover", Map.of("min_size", "50gb", "min_index_age", "24h")))))));
    }

    @Test
    void checkAndCreatePolicy_withoutIsmTemplateSupport_returnsThePolicyId() throws IOException {
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        when(restClient.performRequest(any())).thenThrow(responseException).thenReturn(null);
        when(responseException.getMessage()).thenReturn("Invalid field: [ism_template]");
        final AbstractIndexManager objectUnderTest = createObjectUnderTest(IndexType.GENAI_EVALUATION_PLAIN);

        final Optional<String> policyId = objectUnderTest.checkAndCreatePolicy();

        assertThat(policyId, equalTo(Optional.of(EXPECTED_POLICY_NAME)));
        final ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
        verify(restClient, times(2)).performRequest(requestCaptor.capture());
        assertThat(readPolicy(requestCaptor.getAllValues().get(1)), not(hasKey("ism_template")));
    }

    @Test
    void checkAndCreateIndexTemplate_withIsm_matchesRolloverIndicesAndSetsRolloverAlias() throws IOException {
        stubIndexTemplate();
        final AbstractIndexManager objectUnderTest = createObjectUnderTest(IndexType.GENAI_EVALUATION_PLAIN);
        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME)).thenReturn(Optional.empty());

        objectUnderTest.checkAndCreateIndexTemplate(true, null);

        verify(indexTemplate).setTemplateName(EXPECTED_TEMPLATE_NAME);
        verify(indexTemplate).setIndexPatterns(Collections.singletonList(INDEX_ALIAS + "-*"));
        verify(indexTemplate).putCustomSetting(IndexConstants.ISM_ROLLOVER_ALIAS_SETTING, INDEX_ALIAS);
        verify(indexTemplate, never()).putCustomSetting(eq(IndexConstants.ISM_POLICY_ID_SETTING), anyString());
        verify(templateStrategy).createTemplate(indexTemplate);
    }

    @Test
    void checkAndCreateIndexTemplate_withExistingTemplateOfSameVersion_doesNotOverwriteIt() throws IOException {
        stubIndexTemplate();
        final AbstractIndexManager objectUnderTest = createObjectUnderTest(IndexType.GENAI_EVALUATION_PLAIN);
        when(indexTemplate.getVersion()).thenReturn(Optional.of(1L));
        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME)).thenReturn(Optional.of(1L));

        objectUnderTest.checkAndCreateIndexTemplate(true, null);

        verify(templateStrategy, never()).createTemplate(any());
    }

    @Test
    void checkAndCreateIndex_withIsm_createsFirstIndexWithWriteAlias() throws IOException {
        when(openSearchClient.indices()).thenReturn(openSearchIndicesClient);
        final AbstractIndexManager objectUnderTest = createObjectUnderTest(IndexType.GENAI_EVALUATION_PLAIN);
        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(false));

        objectUnderTest.checkAndCreateIndex();

        final ArgumentCaptor<CreateIndexRequest> requestCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        verify(openSearchIndicesClient).create(requestCaptor.capture());
        final CreateIndexRequest createIndexRequest = requestCaptor.getValue();
        assertThat(createIndexRequest.index(), equalTo(INDEX_ALIAS + "-000001"));
        assertThat(createIndexRequest.aliases().keySet(), equalTo(Set.of(INDEX_ALIAS)));
        assertThat(createIndexRequest.aliases().get(INDEX_ALIAS).isWriteIndex(), equalTo(true));
        verify(openSearchIndicesClient, never()).exists(any(ExistsRequest.class));
    }

    @Test
    void checkAndCreateIndex_withExistingAlias_createsNoIndex() throws IOException {
        when(openSearchClient.indices()).thenReturn(openSearchIndicesClient);
        final AbstractIndexManager objectUnderTest = createObjectUnderTest(IndexType.GENAI_EVALUATION_PLAIN);
        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(true));

        objectUnderTest.checkAndCreateIndex();

        verify(openSearchIndicesClient, never()).create(any(CreateIndexRequest.class));
    }

    @Test
    void checkAndCreatePolicy_onServerless_createsNoPolicy() throws IOException {
        when(indexConfiguration.getServerless()).thenReturn(true);
        final AbstractIndexManager objectUnderTest = createObjectUnderTest(IndexType.GENAI_EVALUATION_PLAIN);

        final Optional<String> policyId = objectUnderTest.checkAndCreatePolicy();

        assertThat(policyId, equalTo(Optional.empty()));
        verify(restHighLevelClient, never()).getLowLevelClient();
    }

    @Test
    void checkAndCreateIndexTemplate_onServerless_matchesOnlyThePlainIndex() throws IOException {
        when(indexConfiguration.getServerless()).thenReturn(true);
        stubIndexTemplate();
        final AbstractIndexManager objectUnderTest = createObjectUnderTest(IndexType.GENAI_EVALUATION_PLAIN);
        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME)).thenReturn(Optional.empty());

        objectUnderTest.checkAndCreateIndexTemplate(false, null);

        verify(indexTemplate).setTemplateName(EXPECTED_TEMPLATE_NAME);
        verify(indexTemplate).setIndexPatterns(Collections.singletonList(INDEX_ALIAS));
        verify(indexTemplate, never()).putCustomSetting(eq(IndexConstants.ISM_POLICY_ID_SETTING), anyString());
        verify(indexTemplate, never()).putCustomSetting(eq(IndexConstants.ISM_ROLLOVER_ALIAS_SETTING), anyString());
        verify(templateStrategy).createTemplate(indexTemplate);
    }

    @Test
    void checkAndCreateIndex_onServerless_createsPlainIndexNamedAfterAlias() throws IOException {
        when(indexConfiguration.getServerless()).thenReturn(true);
        when(openSearchClient.indices()).thenReturn(openSearchIndicesClient);
        final AbstractIndexManager objectUnderTest = createObjectUnderTest(IndexType.GENAI_EVALUATION_PLAIN);
        when(openSearchIndicesClient.exists(any(ExistsRequest.class))).thenReturn(new BooleanResponse(false));

        objectUnderTest.checkAndCreateIndex();

        final ArgumentCaptor<CreateIndexRequest> requestCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        verify(openSearchIndicesClient).create(requestCaptor.capture());
        final CreateIndexRequest createIndexRequest = requestCaptor.getValue();
        assertThat(createIndexRequest.index(), equalTo(INDEX_ALIAS));
        assertThat(createIndexRequest.aliases(), anEmptyMap());
        verify(openSearchIndicesClient, never()).existsAlias(any(ExistsAliasRequest.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void setupIndex_createsTheManagedComponentTemplatesInOrderBeforeTheIndexTemplate(final boolean serverless) throws IOException {
        when(indexConfiguration.getServerless()).thenReturn(serverless);
        final Map<String, Map<String, Object>> componentTemplates = stubManagedComponentTemplates();
        stubIsmEnabled(!serverless);
        stubIndexTemplate();
        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME)).thenReturn(Optional.empty());
        when(openSearchClient.indices()).thenReturn(openSearchIndicesClient);
        if (serverless) {
            when(openSearchIndicesClient.exists(any(ExistsRequest.class))).thenReturn(new BooleanResponse(true));
        } else {
            when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
            when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(true));
        }
        final AbstractIndexManager objectUnderTest = createObjectUnderTest(IndexType.GENAI_EVALUATION_PLAIN);

        objectUnderTest.setupIndex();

        final List<String> componentTemplateNames = new ArrayList<>(componentTemplates.keySet());
        assertThat(componentTemplateNames, equalTo(List.of("otel-v1-genai-evaluation-logs-plain", "otel-v1-genai-evaluation-fields")));
        final InOrder inOrder = inOrder(openSearchTransport, templateStrategy);
        for (final String componentTemplateName : componentTemplateNames) {
            inOrder.verify(openSearchTransport).performRequest(
                    eq(componentTemplates.get(componentTemplateName)), endpointCaptor.capture(), any());
        }
        inOrder.verify(templateStrategy).createTemplate(indexTemplate);
        final List<String> putPaths = new ArrayList<>();
        for (final Endpoint<Map<String, Object>, Object, Object> endpoint : endpointCaptor.getAllValues()) {
            assertThat(endpoint.method(null), equalTo("PUT"));
            putPaths.add(endpoint.requestUrl(null));
        }
        assertThat(putPaths, equalTo(List.of(
                "/_component_template/otel-v1-genai-evaluation-logs-plain",
                "/_component_template/otel-v1-genai-evaluation-fields")));
        verify(indexTemplate).setIndexPatterns(Collections.singletonList(serverless ? INDEX_ALIAS : INDEX_ALIAS + "-*"));
        if (serverless) {
            verify(indexTemplate, never()).putCustomSetting(eq(IndexConstants.ISM_ROLLOVER_ALIAS_SETTING), anyString());
            verify(restHighLevelClient, never()).getLowLevelClient();
        } else {
            verify(indexTemplate).putCustomSetting(IndexConstants.ISM_ROLLOVER_ALIAS_SETTING, INDEX_ALIAS);
        }
    }

    @Test
    void setupIndex_whenAComponentTemplatePutFails_throwsAndCreatesNoIndexTemplateOrIndex() throws IOException {
        when(indexConfiguration.getServerless()).thenReturn(true);
        final Map<String, Map<String, Object>> componentTemplates = stubManagedComponentTemplates();
        stubIsmEnabled(false);
        when(indexConfiguration.getIndexTemplate()).thenReturn(Collections.singletonMap("composed_of", List.of()));
        final OpenSearchException putFailure = openSearchException(400);
        final Map<String, Object> fieldsComponentTemplate = componentTemplates.get("otel-v1-genai-evaluation-fields");
        when(openSearchTransport.performRequest(any(), any(), any())).thenAnswer(invocation -> {
            if (invocation.getArgument(0) == fieldsComponentTemplate) {
                throw putFailure;
            }
            return null;
        });
        final AbstractIndexManager objectUnderTest = createObjectUnderTest(IndexType.GENAI_EVALUATION_PLAIN);

        final OpenSearchException thrown = assertThrows(OpenSearchException.class, objectUnderTest::setupIndex);

        assertThat(thrown, sameInstance(putFailure));
        verify(openSearchTransport).performRequest(eq(componentTemplates.get("otel-v1-genai-evaluation-logs-plain")), any(), any());
        verify(templateStrategy, never()).createTemplate(any());
        verify(openSearchClient, never()).indices();
    }

    @Test
    void setupIndex_whenTheIndexTemplatePutFails_throwsAndCreatesNoIndex() throws IOException {
        when(indexConfiguration.getServerless()).thenReturn(true);
        stubManagedComponentTemplates();
        stubIsmEnabled(false);
        stubIndexTemplate();
        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME)).thenReturn(Optional.empty());
        final OpenSearchException putFailure = openSearchException(500);
        doThrow(putFailure).when(templateStrategy).createTemplate(indexTemplate);
        final AbstractIndexManager objectUnderTest = createObjectUnderTest(IndexType.GENAI_EVALUATION_PLAIN);

        final OpenSearchException thrown = assertThrows(OpenSearchException.class, objectUnderTest::setupIndex);

        assertThat(thrown, sameInstance(putFailure));
        verify(openSearchClient, never()).indices();
    }

    private Map<String, Map<String, Object>> stubManagedComponentTemplates() throws IOException {
        final Map<String, Map<String, Object>> componentTemplates = new IndexConfiguration.Builder()
                .withIndexType(IndexType.GENAI_EVALUATION_PLAIN.getValue())
                .build()
                .getManagedComponentTemplates();
        when(indexConfiguration.getManagedComponentTemplates()).thenReturn(componentTemplates);
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(openSearchClusterClient.existsComponentTemplate(any(ExistsComponentTemplateRequest.class)))
                .thenReturn(new BooleanResponse(false));
        return componentTemplates;
    }

    private void stubIsmEnabled(final boolean ismEnabled) throws IOException {
        when(openSearchClient.cluster()).thenReturn(openSearchClusterClient);
        when(openSearchClusterClient.getSettings(any(GetClusterSettingsRequest.class))).thenReturn(getClusterSettingsResponse);
        when(clusterSettingsParser.getStringValueClusterSetting(getClusterSettingsResponse, IndexConstants.ISM_ENABLED_SETTING))
                .thenReturn(Boolean.toString(ismEnabled));
    }

    private static OpenSearchException openSearchException(final int status) {
        return new OpenSearchException(new ErrorResponse.Builder()
                .status(status)
                .error(new ErrorCause.Builder().type("mapper_parsing_exception").reason("failed to parse mapping").build())
                .build());
    }

    private static Map<String, Object> readPolicy(final Request request) throws IOException {
        try (final InputStream inputStream = request.getEntity().getContent()) {
            final Map<String, Object> policyFile = OBJECT_MAPPER.readValue(inputStream, new TypeReference<Map<String, Object>>() {});
            return (Map<String, Object>) policyFile.get("policy");
        }
    }
}
