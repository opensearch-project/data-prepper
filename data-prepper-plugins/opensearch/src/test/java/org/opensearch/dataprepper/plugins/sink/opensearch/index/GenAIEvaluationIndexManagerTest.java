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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsAliasRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class GenAIEvaluationIndexManagerTest {
    private static final String INDEX_ALIAS = IndexConstants.GENAI_EVALUATION_DEFAULT_ALIAS;
    private static final String EXPECTED_TEMPLATE_NAME = INDEX_ALIAS + "-index-template";

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

    @BeforeEach
    void setup() {
        indexManagerFactory = new IndexManagerFactory(clusterSettingsParser);
        when(openSearchSinkConfiguration.getIndexConfiguration()).thenReturn(indexConfiguration);
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS);
        final Map<String, Object> indexTemplateMap = Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        when(indexConfiguration.getIndexTemplate()).thenReturn(indexTemplateMap);
        when(templateStrategy.createIndexTemplate(indexTemplateMap)).thenReturn(indexTemplate);
        when(openSearchClient.indices()).thenReturn(openSearchIndicesClient);
    }

    private AbstractIndexManager createObjectUnderTest(final IndexType indexType) {
        return indexManagerFactory.getIndexManager(
                indexType, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
    }

    @Test
    void checkAndCreatePolicy_withGenAIEvaluationPlain_createsNoPolicy() throws IOException {
        final AbstractIndexManager objectUnderTest = createObjectUnderTest(IndexType.GENAI_EVALUATION_PLAIN);

        final Optional<String> policyId = objectUnderTest.checkAndCreatePolicy();

        assertThat(policyId, equalTo(Optional.empty()));
        verify(restHighLevelClient, never()).getLowLevelClient();
    }

    @Test
    void checkAndCreateIndexTemplate_withGenAIEvaluationPlain_matchesOnlyTheIndex() throws IOException {
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
    void checkAndCreateIndex_withGenAIEvaluationPlain_createsIndexNamedAfterAlias() throws IOException {
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

}
