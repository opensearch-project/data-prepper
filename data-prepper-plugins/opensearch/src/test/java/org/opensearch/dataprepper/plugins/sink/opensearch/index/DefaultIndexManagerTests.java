/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.apache.http.HttpStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.cluster.GetClusterSettingsRequest;
import org.opensearch.client.opensearch.cluster.GetClusterSettingsResponse;
import org.opensearch.client.opensearch.cluster.OpenSearchClusterClient;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsAliasRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.opensearch.indices.GetTemplateRequest;
import org.opensearch.client.opensearch.indices.GetTemplateResponse;
import org.opensearch.client.opensearch.indices.OpenSearchIndicesClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.PreSerializedJsonpMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
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

public class DefaultIndexManagerTests {

    private static final String INDEX_ALIAS = "test-index-alias";
    private static final String EXPECTED_TEMPLATE_NAME = INDEX_ALIAS + "-index-template";
    private static final String INDEX_ALIAS_WITH_TIME_PATTERN = INDEX_ALIAS+ "-%{yyyy.MM.dd.HH}";
    private static final Pattern EXPECTED_INDEX_PATTERN = Pattern.compile(INDEX_ALIAS + "-\\d{4}.\\d{2}.\\d{2}.\\d{2}");

    private IndexManagerFactory indexManagerFactory;

    private AbstractIndexManager defaultIndexManager;

    @Mock
    private RestHighLevelClient restHighLevelClient;

    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    private OpenSearchSinkConfiguration openSearchSinkConfiguration;

    @Mock
    private ClusterSettingsParser clusterSettingsParser;

    @Mock
    private OpenSearchClusterClient openSearchClusterClient;

    @Mock
    private GetClusterSettingsResponse getClusterSettingsResponse;

    @Mock
    private IndexConfiguration indexConfiguration;

    @Mock
    private OpenSearchIndicesClient openSearchIndicesClient;

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

    @Mock
    private TemplateStrategy templateStrategy;
    private Map<String, Object> indexTemplateMap;
    @Mock
    private IndexTemplate indexTemplateObject;

    @BeforeEach
    void setup() throws IOException {
        initMocks(this);

        indexTemplateMap = Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        indexManagerFactory = new IndexManagerFactory(clusterSettingsParser);
        when(openSearchSinkConfiguration.getIndexConfiguration()).thenReturn(indexConfiguration);
        when(indexConfiguration.getIsmPolicyFile()).thenReturn(Optional.empty());
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS);
        when(indexConfiguration.getIndexTemplate())
                .thenReturn(indexTemplateMap);
        when(openSearchClient.cluster()).thenReturn(openSearchClusterClient);
        when(openSearchClusterClient.getSettings(any(GetClusterSettingsRequest.class)))
                .thenReturn(getClusterSettingsResponse);
        when(openSearchClient.indices()).thenReturn(openSearchIndicesClient);
        when(templateStrategy.createIndexTemplate(indexTemplateMap))
                .thenReturn(indexTemplateObject);
    }

    @Test
    void getIndexAlias_IndexWithTimePattern() throws IOException {
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS_WITH_TIME_PATTERN);
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);

        final String indexAlias = defaultIndexManager.getIndexName(null);
        assertThat(indexAlias, matchesPattern(EXPECTED_INDEX_PATTERN));

        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(indexConfiguration).getIsmPolicyFile();
    }

    @Test
    void getIndexAlias_IndexWithTimePattern_NotAsSuffix() throws IOException {
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS_WITH_TIME_PATTERN + "-randomtext");
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);

        final String indexAlias = defaultIndexManager.getIndexName(null);
        assertThat(indexAlias, matchesPattern(Pattern.compile(INDEX_ALIAS + "-\\d{4}.\\d{2}.\\d{2}.\\d{2}-randomtext")));

        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(indexConfiguration).getIsmPolicyFile();
    }

    private static final List<Character> INVALID_CHARS = Arrays.asList('#', '\\', '/', '*', '?', '"', '<', '>', '|', ',', ':');
    @Test
    void getIndexAlias_IndexWithTimePattern_Exceptional_WithSpecialChars() {
        INVALID_CHARS.stream().forEach(this::testIndexTimePattern_Exceptional_WithSpecialChars);
        verify(openSearchSinkConfiguration, times(INVALID_CHARS.size())).getIndexConfiguration();
        verify(indexConfiguration, times(INVALID_CHARS.size())).getIndexAlias();
    }

    private void testIndexTimePattern_Exceptional_WithSpecialChars(final Character character){
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS + "-%{yyyy" + character + ".MM.dd.HH}");
        assertThrows(IllegalArgumentException.class,
                () -> indexManagerFactory.getIndexManager(
                        IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy));
    }

    @Test
    void testIndexTimePattern_Exceptional_MultipleTimePatterns(){
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS + "-%{yyyy}-%{MM.dd.HH}");
        assertThrows(IllegalArgumentException.class,
                () -> indexManagerFactory.getIndexManager(
                        IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void testIndexTimePattern_Exceptional_NestedPatterns(){
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS + "-%{%{yyyy.MM.dd}}");
        assertThrows(IllegalArgumentException.class,
                () -> indexManagerFactory.getIndexManager(
                        IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy));
        verify(openSearchSinkConfiguration).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    private static final List<Character> UNSUPPORTED_TIME_GRANULARITY_CHARS = Arrays.asList('m', 's', 'S', 'A', 'n', 'N');
    @Test
    void getIndexAlias_IndexWithTimePattern_TooGranular() {
        UNSUPPORTED_TIME_GRANULARITY_CHARS.stream().forEach(this::testIndexTimePattern_Exceptional_TooGranular);
        verify(openSearchSinkConfiguration, times(UNSUPPORTED_TIME_GRANULARITY_CHARS.size())).getIndexConfiguration();
        verify(indexConfiguration, times(UNSUPPORTED_TIME_GRANULARITY_CHARS.size())).getIndexAlias();
    }

    private void testIndexTimePattern_Exceptional_TooGranular(final Character character){
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS + "-%{yyyy.MM.dd.HH."+ character + "}");
        assertThrows(IllegalArgumentException.class,
                () -> indexManagerFactory.getIndexManager(
                        IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy));
    }

    @Test
    void checkAndCreateIndex_IndexWithTimePattern_AlreadyExists() throws IOException {
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS_WITH_TIME_PATTERN);
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        final ArgumentCaptor<ExistsRequest> existsRequestArgumentCaptor = ArgumentCaptor.forClass(ExistsRequest.class);
        when(openSearchIndicesClient.exists(existsRequestArgumentCaptor.capture())).thenReturn(
                new BooleanResponse(true));

        defaultIndexManager.checkAndCreateIndex();

        final String index = existsRequestArgumentCaptor.getValue().index().get(0);
        assertTrue(EXPECTED_INDEX_PATTERN.matcher(index).matches());

        verify(indexConfiguration).getIsmPolicyFile();
        verify(openSearchClient).indices();
        verify(openSearchIndicesClient).exists(any(ExistsRequest.class));
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void checkAndCreateIndex_IndexWithTimePattern_NeedToCreateNewIndex() throws IOException {
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS_WITH_TIME_PATTERN);
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        final ArgumentCaptor<CreateIndexRequest> createIndexRequestCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        when(openSearchIndicesClient.exists(any(ExistsRequest.class))).thenReturn(
                new BooleanResponse(false));
        when(openSearchIndicesClient.create(createIndexRequestCaptor.capture())).thenReturn(null);
        defaultIndexManager.checkAndCreateIndex();
        final String index = createIndexRequestCaptor.getValue().index();
        assertTrue(EXPECTED_INDEX_PATTERN.matcher(index).matches());
        verify(indexConfiguration).getIsmPolicyFile();
        verify(openSearchClient, times(2)).indices();
        verify(openSearchIndicesClient).exists(any(ExistsRequest.class));
        verify(openSearchIndicesClient).create(any(CreateIndexRequest.class));
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void checkAndCreateIndex_IndexWithTimePattern_CreateNewIndex_Exception() throws IOException {
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS_WITH_TIME_PATTERN);
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        final ArgumentCaptor<CreateIndexRequest> createIndexRequestCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        when(openSearchIndicesClient.exists(any(ExistsRequest.class))).thenReturn(
                new BooleanResponse(false));
        when(openSearchException.getMessage()).thenReturn("");
        when(openSearchIndicesClient.create(createIndexRequestCaptor.capture())).thenThrow(openSearchException);
        try {
            defaultIndexManager.checkAndCreateIndex();
        } catch (final IOException e) {
            verify(indexConfiguration).getIsmPolicyFile();
            verify(openSearchClient, times(2)).indices();
            verify(openSearchIndicesClient).exists(any(ExistsRequest.class));
            verify(openSearchIndicesClient).create(any(CreateIndexRequest.class));

            final String index = createIndexRequestCaptor.getValue().index();
            assertTrue(EXPECTED_INDEX_PATTERN.matcher(index).matches());

            verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
            verify(indexConfiguration).getIndexAlias();
        }
    }

    @Test
    void getIndexAlias_IndexWithoutTimePattern() throws IOException {
        when(indexConfiguration.getIndexAlias()).thenReturn(INDEX_ALIAS);
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);

        final String indexAlias = defaultIndexManager.getIndexName(null);
        assertEquals(INDEX_ALIAS, indexAlias);

        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(indexConfiguration).getIsmPolicyFile();
    }

    @Test
    void constructor_NullRestClient() {
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        assertThrows(NullPointerException.class, () ->
                indexManagerFactory.getIndexManager(
                        IndexType.CUSTOM, openSearchClient, null, openSearchSinkConfiguration, templateStrategy));
        verify(indexConfiguration).getIndexAlias();
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIsmPolicyFile();
    }

    @Test
    void constructor_NullConfiguration() {
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        assertThrows(NullPointerException.class, () ->
                indexManagerFactory.getIndexManager(
                        IndexType.CUSTOM, openSearchClient, restHighLevelClient, null, templateStrategy));
        verify(indexConfiguration).getIndexAlias();
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIsmPolicyFile();
    }

    @Test
    void isIndexAlias_True() throws IOException {
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(true));
        when(clusterSettingsParser.getStringValueClusterSetting(any(GetClusterSettingsResponse.class), anyString())).thenReturn("true");
        assertEquals(true, defaultIndexManager.isIndexAlias(INDEX_ALIAS));
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIsmPolicyFile();
        verify(indexConfiguration).getIndexAlias();
        verify(openSearchClient).indices();
        verify(openSearchIndicesClient).existsAlias(any(ExistsAliasRequest.class));
        verify(openSearchClient).cluster();
        verify(openSearchClusterClient).getSettings(any(GetClusterSettingsRequest.class));
    }

    @Test
    void isIndexAlias_False_NoAlias() throws IOException {
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(false));
        assertEquals(false, defaultIndexManager.isIndexAlias(INDEX_ALIAS));
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIsmPolicyFile();
        verify(indexConfiguration).getIndexAlias();
        verify(openSearchClient).indices();
        verify(openSearchIndicesClient).existsAlias(any(ExistsAliasRequest.class));
    }

    @Test
    void isIndexAlias_False_NoISM() throws IOException {
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        when(openSearchIndicesClient.existsAlias(any(ExistsAliasRequest.class))).thenReturn(new BooleanResponse(true));
        when(clusterSettingsParser.getStringValueClusterSetting(any(GetClusterSettingsResponse.class), anyString())).thenReturn("false");
        assertEquals(false, defaultIndexManager.isIndexAlias(INDEX_ALIAS));
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIsmPolicyFile();
        verify(indexConfiguration).getIndexAlias();
        verify(openSearchClient).indices();
        verify(openSearchIndicesClient).existsAlias(any(ExistsAliasRequest.class));
        verify(openSearchClient).cluster();
        verify(openSearchClusterClient).getSettings(any(GetClusterSettingsRequest.class));
    }

    @Test
    void checkISMEnabled_True() throws IOException {
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        when(clusterSettingsParser.getStringValueClusterSetting(any(GetClusterSettingsResponse.class), anyString())).thenReturn("true");
        assertEquals(true, defaultIndexManager.checkISMEnabled());
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIsmPolicyFile();
        verify(indexConfiguration).getIndexAlias();
        verify(openSearchClient).cluster();
        verify(openSearchClusterClient).getSettings(any(GetClusterSettingsRequest.class));
    }

    @Test
    void checkISMEnabled_False() throws IOException {
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        when(clusterSettingsParser.getStringValueClusterSetting(any(GetClusterSettingsResponse.class), anyString())).thenReturn("false");
        assertEquals(false, defaultIndexManager.checkISMEnabled());
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIsmPolicyFile();
        verify(indexConfiguration).getIndexAlias();
        verify(openSearchClient).cluster();
        verify(openSearchClusterClient).getSettings(any(GetClusterSettingsRequest.class));
    }

    @ParameterizedTest
    @ValueSource(ints = {HttpStatus.SC_NOT_FOUND, HttpStatus.SC_BAD_REQUEST})
    void checkISMEnabled_FalseWhenOpenSearchExceptionStatusNonRetryable(final int statusCode) throws IOException {
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        when(openSearchException.status()).thenReturn(statusCode);
        when(openSearchClusterClient.getSettings(any(GetClusterSettingsRequest.class))).thenThrow(openSearchException);
        assertEquals(false, defaultIndexManager.checkISMEnabled());
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIsmPolicyFile();
        verify(indexConfiguration).getIndexAlias();
        verify(openSearchClient).cluster();
        verify(openSearchClusterClient).getSettings(any(GetClusterSettingsRequest.class));
    }

    @Test
    void checkAndCreatePolicy_Normal() throws IOException {
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        when(indexConfiguration.getIsmPolicyFile()).thenReturn(Optional.of("test-custom-index-policy-file.json"));
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        assertEquals(Optional.empty(), defaultIndexManager.checkAndCreatePolicy(INDEX_ALIAS));
        verify(restHighLevelClient).getLowLevelClient();
        verify(restClient).performRequest(any());
        verify(openSearchSinkConfiguration, times(4)).getIndexConfiguration();
        verify(indexConfiguration, times(2)).getIsmPolicyFile();
        verify(indexConfiguration, times(2)).getIndexAlias();
    }

    @Test
    void checkAndCreatePolicy_Exception() throws IOException {
        when(indexConfiguration.getIsmPolicyFile()).thenReturn(Optional.of("test-custom-index-policy-file.json"));
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        when(restHighLevelClient.getLowLevelClient()).thenReturn(restClient);
        when(restClient.performRequest(any())).thenThrow(responseException);
        when(responseException.getMessage()).thenReturn("Invalid field: [ism_template]");
        assertThrows(ResponseException.class, () -> defaultIndexManager.checkAndCreatePolicy(INDEX_ALIAS));
        verify(restHighLevelClient, times(2)).getLowLevelClient();
        verify(restClient, times(2)).performRequest(any());
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIsmPolicyFile();
        verify(indexConfiguration).getIndexAlias();
        verify(responseException, times(3)).getMessage();
    }

    @Test
    void checkAndCreatePolicy() throws IOException {
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        assertEquals(Optional.empty(), defaultIndexManager.checkAndCreatePolicy(INDEX_ALIAS));
        verify(indexConfiguration).getIndexAlias();
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIsmPolicyFile();
    }

    @Test
    void checkAndCreateIndexTemplate_NoIndexTemplateOnHost_ISMDisabled() throws IOException {
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        when(booleanResponse.value()).thenReturn(false);
        when(openSearchTransport.jsonpMapper()).thenReturn(new PreSerializedJsonpMapper());
        when(openSearchClient._transport()).thenReturn(openSearchTransport);

        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME))
                .thenReturn(Optional.empty());

        defaultIndexManager.checkAndCreateIndexTemplate(false, null);
        verify(openSearchSinkConfiguration, times(3)).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(indexConfiguration).getIsmPolicyFile();
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

        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        defaultIndexManager.checkAndCreateIndexTemplate(true, null);
        verify(indexConfiguration).getIsmPolicyFile();
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
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        when(booleanResponse.value()).thenReturn(true);
        when(openSearchTransport.jsonpMapper()).thenReturn(new PreSerializedJsonpMapper());
        when(openSearchClient._transport()).thenReturn(openSearchTransport);
        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME))
                .thenReturn(Optional.of(0L));
        when(openSearchIndicesClient.getTemplate(any(GetTemplateRequest.class))).thenReturn(getTemplateResponse);

        defaultIndexManager.checkAndCreateIndexTemplate(false, null);
        verify(indexConfiguration).getIsmPolicyFile();
        verify(openSearchSinkConfiguration, atLeastOnce()).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(indexConfiguration).getIndexTemplate();
        verify(templateStrategy).getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME);
        verify(templateStrategy, never()).createTemplate(any());
    }

    @Test
    void checkAndCreateIndex_IndexAlreadyExists() throws IOException {
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        when(openSearchIndicesClient.exists(any(ExistsRequest.class))).thenReturn(new BooleanResponse(true));
        when(templateStrategy.getExistingTemplateVersion(EXPECTED_TEMPLATE_NAME))
                .thenReturn(Optional.of(0L));
        defaultIndexManager.checkAndCreateIndex();
        verify(indexConfiguration).getIsmPolicyFile();
        verify(openSearchClient).indices();
        verify(openSearchIndicesClient).exists(any(ExistsRequest.class));
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
        verify(templateStrategy, never()).createTemplate(any());
    }

    @Test
    void checkAndCreateIndex_NeedToCreateNewIndex() throws IOException {
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        when(openSearchIndicesClient.exists(any(ExistsRequest.class))).thenReturn(new BooleanResponse(false));
        when(openSearchIndicesClient.create(any(CreateIndexRequest.class))).thenReturn(null);

        defaultIndexManager.checkAndCreateIndex();
        verify(indexConfiguration).getIsmPolicyFile();
        verify(openSearchClient, times(2)).indices();
        verify(openSearchIndicesClient).exists(any(ExistsRequest.class));
        verify(openSearchIndicesClient).create(any(CreateIndexRequest.class));
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @Test
    void checkAndCreateIndex_CreateNewIndex_Exception() throws IOException {
        defaultIndexManager = indexManagerFactory.getIndexManager(
                IndexType.CUSTOM, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy);
        when(openSearchIndicesClient.exists(any(ExistsRequest.class))).thenReturn(new BooleanResponse(false));
        when(openSearchException.getMessage()).thenReturn("");
        when(openSearchIndicesClient.create(any(CreateIndexRequest.class))).thenThrow(openSearchException);

        assertThrows(IOException.class, () -> defaultIndexManager.checkAndCreateIndex());

        verify(indexConfiguration).getIsmPolicyFile();
        verify(openSearchClient, times(2)).indices();
        verify(openSearchIndicesClient).exists(any(ExistsRequest.class));
        verify(openSearchIndicesClient).create(any(CreateIndexRequest.class));
        verify(openSearchSinkConfiguration, times(2)).getIndexConfiguration();
        verify(indexConfiguration).getIndexAlias();
    }

    @AfterEach
    void clear() {
        verifyNoMoreInteractions(
                restHighLevelClient,
                openSearchSinkConfiguration,
                openSearchClusterClient,
                indexConfiguration,
                openSearchIndicesClient,
                getTemplateResponse,
                restClient,
                responseException
        );
    }
}
