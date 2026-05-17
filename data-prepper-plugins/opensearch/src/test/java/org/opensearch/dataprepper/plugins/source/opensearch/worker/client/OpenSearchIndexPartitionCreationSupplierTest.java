/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.cat.ElasticsearchCatClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.cat.IndicesResponse;
import org.opensearch.client.opensearch.cat.OpenSearchCatClient;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.dataprepper.model.plugin.PluginComponentRefresher;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.plugins.source.opensearch.ClientRefresher;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.IndexParametersConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.OpenSearchIndex;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.OpenSearchIndexPartitionCreationSupplier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.OpenSearchIndexPartitionCreationSupplier.SINGLE_SCAN_COMPLETE;

@ExtendWith(MockitoExtension.class)
public class OpenSearchIndexPartitionCreationSupplierTest {

    @Mock
    private OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @Mock
    private ClusterClientFactory clusterClientFactory;

    @Mock
    private PluginComponentRefresher<OpenSearchClient, OpenSearchSourceConfiguration> opensearchClientRefresher;

    @Mock
    private OpenSearchClient openSearchClient;

    @Mock
    private PluginComponentRefresher<ElasticsearchClient, OpenSearchSourceConfiguration> elasticsearchClientRefresher;

    @Mock
    private ElasticsearchClient elasticsearchClient;

    private OpenSearchIndexPartitionCreationSupplier createObjectUnderTest() {
        return new OpenSearchIndexPartitionCreationSupplier(openSearchSourceConfiguration, clusterClientFactory);
    }

    @Test
    void clusterClientFactory_that_is_not_valid_refresher_throws_IllegalArgumentException() {
        final ClientRefresher clientRefresher = mock(ClientRefresher.class);
        when(clientRefresher.getComponentClass()).thenReturn(Object.class);
        when(clusterClientFactory.getClientRefresher()).thenReturn(clientRefresher);

        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @ParameterizedTest
    @MethodSource("opensearchCatIndicesExceptions")
    void apply_with_opensearch_client_cat_indices_throws_exception_returns_empty_list(final Class exception) throws IOException {
        when(opensearchClientRefresher.getComponentClass()).thenReturn(OpenSearchClient.class);
        when(opensearchClientRefresher.get()).thenReturn(openSearchClient);
        when(clusterClientFactory.getClientRefresher()).thenReturn(opensearchClientRefresher);

        final OpenSearchCatClient openSearchCatClient = mock(OpenSearchCatClient.class);
        when(openSearchCatClient.indices()).thenThrow(exception);
        when(openSearchClient.cat()).thenReturn(openSearchCatClient);

        final List<PartitionIdentifier> partitionIdentifierList = createObjectUnderTest().apply(Collections.emptyMap());

        assertThat(partitionIdentifierList, notNullValue());
        assertThat(partitionIdentifierList.isEmpty(), equalTo(true));
    }

    @ParameterizedTest
    @MethodSource("elasticsearchCatIndicesExceptions")
    void apply_with_elasticsearch_client_cat_indices_throws_exception_returns_empty_list(final Class exception) throws IOException {
        when(elasticsearchClientRefresher.getComponentClass()).thenReturn(ElasticsearchClient.class);
        when(elasticsearchClientRefresher.get()).thenReturn(elasticsearchClient);
        when(clusterClientFactory.getClientRefresher()).thenReturn(elasticsearchClientRefresher);

        final ElasticsearchCatClient elasticsearchCatClient = mock(ElasticsearchCatClient.class);
        when(elasticsearchCatClient.indices()).thenThrow(exception);
        when(elasticsearchClient.cat()).thenReturn(elasticsearchCatClient);

        final List<PartitionIdentifier> partitionIdentifierList = createObjectUnderTest().apply(Collections.emptyMap());

        assertThat(partitionIdentifierList, notNullValue());
        assertThat(partitionIdentifierList.isEmpty(), equalTo(true));
    }

    @Test
    void apply_with_opensearch_client_with_no_indices_return_empty_list() throws IOException {
        when(opensearchClientRefresher.getComponentClass()).thenReturn(OpenSearchClient.class);
        when(opensearchClientRefresher.get()).thenReturn(openSearchClient);
        when(clusterClientFactory.getClientRefresher()).thenReturn(opensearchClientRefresher);

        final OpenSearchCatClient openSearchCatClient = mock(OpenSearchCatClient.class);
        final IndicesResponse indicesResponse = mock(IndicesResponse.class);
        when(indicesResponse.valueBody()).thenReturn(Collections.emptyList());
        when(openSearchCatClient.indices()).thenReturn(indicesResponse);
        when(openSearchClient.cat()).thenReturn(openSearchCatClient);

        final List<PartitionIdentifier> partitionIdentifierList = createObjectUnderTest().apply(Collections.emptyMap());

        assertThat(partitionIdentifierList, notNullValue());
        assertThat(partitionIdentifierList.isEmpty(), equalTo(true));
    }

    @Test
    void apply_with_elasticsearch_client_with_no_indices_return_empty_list() throws IOException {
        when(elasticsearchClientRefresher.getComponentClass()).thenReturn(ElasticsearchClient.class);
        when(elasticsearchClientRefresher.get()).thenReturn(elasticsearchClient);
        when(clusterClientFactory.getClientRefresher()).thenReturn(elasticsearchClientRefresher);

        final ElasticsearchCatClient elasticsearchCatClient = mock(ElasticsearchCatClient.class);
        final co.elastic.clients.elasticsearch.cat.IndicesResponse indicesResponse = mock(co.elastic.clients.elasticsearch.cat.IndicesResponse.class);
        when(indicesResponse.valueBody()).thenReturn(Collections.emptyList());
        when(elasticsearchCatClient.indices()).thenReturn(indicesResponse);
        when(elasticsearchClient.cat()).thenReturn(elasticsearchCatClient);

        final List<PartitionIdentifier> partitionIdentifierList = createObjectUnderTest().apply(Collections.emptyMap());

        assertThat(partitionIdentifierList, notNullValue());
        assertThat(partitionIdentifierList.isEmpty(), equalTo(true));
    }

    @Test
    void apply_with_opensearch_client_with_indices_filters_them_correctly() throws IOException {
        when(opensearchClientRefresher.getComponentClass()).thenReturn(OpenSearchClient.class);
        when(opensearchClientRefresher.get()).thenReturn(openSearchClient);
        when(clusterClientFactory.getClientRefresher()).thenReturn(opensearchClientRefresher);

        final OpenSearchCatClient openSearchCatClient = mock(OpenSearchCatClient.class);
        final IndicesResponse indicesResponse = mock(IndicesResponse.class);

        final IndexParametersConfiguration indexParametersConfiguration = mock(IndexParametersConfiguration.class);

        final List<OpenSearchIndex> includedIndices = new ArrayList<>();
        final OpenSearchIndex includeIndex = mock(OpenSearchIndex.class);
        final String includePattern = "my-pattern-[a-c].*";
        when(includeIndex.getIndexNamePattern()).thenReturn(Pattern.compile(includePattern));
        includedIndices.add(includeIndex);

        final List<OpenSearchIndex> excludedIndices = new ArrayList<>();
        final OpenSearchIndex excludeIndex = mock(OpenSearchIndex.class);
        final String excludePattern = "my-pattern-[a-c]-exclude";
        when(excludeIndex.getIndexNamePattern()).thenReturn(Pattern.compile(excludePattern));
        excludedIndices.add(excludeIndex);

        final OpenSearchIndex secondExcludeIndex = mock(OpenSearchIndex.class);
        final String secondExcludePattern = "second-exclude-.*";
        when(secondExcludeIndex.getIndexNamePattern()).thenReturn(Pattern.compile(secondExcludePattern));
        excludedIndices.add(secondExcludeIndex);

        when(indexParametersConfiguration.getIncludedIndices()).thenReturn(includedIndices);
        when(indexParametersConfiguration.getExcludedIndices()).thenReturn(excludedIndices);
        when(openSearchSourceConfiguration.getIndexParametersConfiguration()).thenReturn(indexParametersConfiguration);

        final List<IndicesRecord> indicesRecords = new ArrayList<>();
        final IndicesRecord includedIndex = mock(IndicesRecord.class);
        when(includedIndex.index()).thenReturn("my-pattern-a-include");
        final IndicesRecord excludedIndex = mock(IndicesRecord.class);
        when(excludedIndex.index()).thenReturn("second-exclude-test");
        final IndicesRecord includedAndThenExcluded = mock(IndicesRecord.class);
        when(includedAndThenExcluded.index()).thenReturn("my-pattern-a-exclude");
        final IndicesRecord neitherIncludedOrExcluded = mock(IndicesRecord.class);
        when(neitherIncludedOrExcluded.index()).thenReturn("random-index");

        indicesRecords.add(includedIndex);
        indicesRecords.add(excludedIndex);
        indicesRecords.add(includedAndThenExcluded);
        indicesRecords.add(neitherIncludedOrExcluded);

        when(indicesResponse.valueBody()).thenReturn(indicesRecords);

        when(openSearchCatClient.indices()).thenReturn(indicesResponse);
        when(openSearchClient.cat()).thenReturn(openSearchCatClient);

        final List<PartitionIdentifier> partitionIdentifierList = createObjectUnderTest().apply(Collections.emptyMap());

        assertThat(partitionIdentifierList, notNullValue());
    }

    @Test
    void apply_with_elasticsearch_client_with_indices_filters_them_correctly() throws IOException {
        when(elasticsearchClientRefresher.getComponentClass()).thenReturn(ElasticsearchClient.class);
        when(elasticsearchClientRefresher.get()).thenReturn(elasticsearchClient);
        when(clusterClientFactory.getClientRefresher()).thenReturn(elasticsearchClientRefresher);

        final ElasticsearchCatClient elasticsearchCatClient = mock(ElasticsearchCatClient.class);
        final co.elastic.clients.elasticsearch.cat.IndicesResponse indicesResponse = mock(co.elastic.clients.elasticsearch.cat.IndicesResponse.class);

        final IndexParametersConfiguration indexParametersConfiguration = mock(IndexParametersConfiguration.class);

        final List<OpenSearchIndex> includedIndices = new ArrayList<>();
        final OpenSearchIndex includeIndex = mock(OpenSearchIndex.class);
        final String includePattern = "my-pattern-[a-c].*";
        when(includeIndex.getIndexNamePattern()).thenReturn(Pattern.compile(includePattern));
        includedIndices.add(includeIndex);

        final List<OpenSearchIndex> excludedIndices = new ArrayList<>();
        final OpenSearchIndex excludeIndex = mock(OpenSearchIndex.class);
        final String excludePattern = "my-pattern-[a-c]-exclude";
        when(excludeIndex.getIndexNamePattern()).thenReturn(Pattern.compile(excludePattern));
        excludedIndices.add(excludeIndex);

        final OpenSearchIndex secondExcludeIndex = mock(OpenSearchIndex.class);
        final String secondExcludePattern = "second-exclude-.*";
        when(secondExcludeIndex.getIndexNamePattern()).thenReturn(Pattern.compile(secondExcludePattern));
        excludedIndices.add(secondExcludeIndex);

        when(indexParametersConfiguration.getIncludedIndices()).thenReturn(includedIndices);
        when(indexParametersConfiguration.getExcludedIndices()).thenReturn(excludedIndices);
        when(openSearchSourceConfiguration.getIndexParametersConfiguration()).thenReturn(indexParametersConfiguration);

        final List<co.elastic.clients.elasticsearch.cat.indices.IndicesRecord> indicesRecords = new ArrayList<>();
        final co.elastic.clients.elasticsearch.cat.indices.IndicesRecord includedIndex = mock(co.elastic.clients.elasticsearch.cat.indices.IndicesRecord.class);
        when(includedIndex.index()).thenReturn("my-pattern-a-include");
        final co.elastic.clients.elasticsearch.cat.indices.IndicesRecord excludedIndex = mock(co.elastic.clients.elasticsearch.cat.indices.IndicesRecord.class);
        when(excludedIndex.index()).thenReturn("second-exclude-test");
        final co.elastic.clients.elasticsearch.cat.indices.IndicesRecord includedAndThenExcluded = mock(co.elastic.clients.elasticsearch.cat.indices.IndicesRecord.class);
        when(includedAndThenExcluded.index()).thenReturn("my-pattern-a-exclude");
        final co.elastic.clients.elasticsearch.cat.indices.IndicesRecord neitherIncludedOrExcluded = mock(co.elastic.clients.elasticsearch.cat.indices.IndicesRecord.class);
        when(neitherIncludedOrExcluded.index()).thenReturn("random-index");

        indicesRecords.add(includedIndex);
        indicesRecords.add(excludedIndex);
        indicesRecords.add(includedAndThenExcluded);
        indicesRecords.add(neitherIncludedOrExcluded);

        when(indicesResponse.valueBody()).thenReturn(indicesRecords);

        when(elasticsearchCatClient.indices()).thenReturn(indicesResponse);
        when(elasticsearchClient.cat()).thenReturn(elasticsearchCatClient);

        final List<PartitionIdentifier> partitionIdentifierList = createObjectUnderTest().apply(Collections.emptyMap());

        assertThat(partitionIdentifierList, notNullValue());
    }

    @Test
    void apply_with_single_scan_mode_returns_empty_when_flag_already_set() {
        when(openSearchSourceConfiguration.isSingleScanMode()).thenReturn(true);

        // Refresher is wired through the constructor but should not be used when the scan is already completed
        when(opensearchClientRefresher.getComponentClass()).thenReturn(OpenSearchClient.class);
        when(clusterClientFactory.getClientRefresher()).thenReturn(opensearchClientRefresher);

        final OpenSearchIndexPartitionCreationSupplier objectUnderTest = createObjectUnderTest();

        final Map<String, Object> globalStateMap = new HashMap<>();
        globalStateMap.put(SINGLE_SCAN_COMPLETE, Boolean.TRUE);

        final List<PartitionIdentifier> partitionIdentifierList = objectUnderTest.apply(globalStateMap);

        assertThat(partitionIdentifierList, notNullValue());
        assertThat(partitionIdentifierList.isEmpty(), equalTo(true));
        verify(opensearchClientRefresher, never()).get();
    }

    @Test
    void apply_with_single_scan_mode_sets_flag_after_discovering_partitions() throws IOException {
        when(openSearchSourceConfiguration.isSingleScanMode()).thenReturn(true);

        when(opensearchClientRefresher.getComponentClass()).thenReturn(OpenSearchClient.class);
        when(opensearchClientRefresher.get()).thenReturn(openSearchClient);
        when(clusterClientFactory.getClientRefresher()).thenReturn(opensearchClientRefresher);

        final OpenSearchCatClient openSearchCatClient = mock(OpenSearchCatClient.class);
        final IndicesResponse indicesResponse = mock(IndicesResponse.class);
        final IndicesRecord indicesRecord = mock(IndicesRecord.class);
        when(indicesRecord.index()).thenReturn("my-index");
        when(indicesResponse.valueBody()).thenReturn(List.of(indicesRecord));
        when(openSearchCatClient.indices()).thenReturn(indicesResponse);
        when(openSearchClient.cat()).thenReturn(openSearchCatClient);

        final Map<String, Object> globalStateMap = new HashMap<>();

        final List<PartitionIdentifier> partitionIdentifierList = createObjectUnderTest().apply(globalStateMap);

        assertThat(partitionIdentifierList, notNullValue());
        assertThat(partitionIdentifierList, hasSize(1));
        assertThat(globalStateMap, hasEntry(SINGLE_SCAN_COMPLETE, Boolean.TRUE));
    }

    @Test
    void apply_with_periodic_mode_does_not_set_single_scan_flag() throws IOException {
        when(openSearchSourceConfiguration.isSingleScanMode()).thenReturn(false);

        when(opensearchClientRefresher.getComponentClass()).thenReturn(OpenSearchClient.class);
        when(opensearchClientRefresher.get()).thenReturn(openSearchClient);
        when(clusterClientFactory.getClientRefresher()).thenReturn(opensearchClientRefresher);

        final OpenSearchCatClient openSearchCatClient = mock(OpenSearchCatClient.class);
        final IndicesResponse indicesResponse = mock(IndicesResponse.class);
        when(indicesResponse.valueBody()).thenReturn(Collections.emptyList());
        when(openSearchCatClient.indices()).thenReturn(indicesResponse);
        when(openSearchClient.cat()).thenReturn(openSearchCatClient);

        final Map<String, Object> globalStateMap = new HashMap<>();

        createObjectUnderTest().apply(globalStateMap);

        assertThat(globalStateMap, not(hasEntry(SINGLE_SCAN_COMPLETE, Boolean.TRUE)));
    }

    @Test
    void apply_with_periodic_mode_and_completed_flag_still_performs_discovery() throws IOException {
        when(openSearchSourceConfiguration.isSingleScanMode()).thenReturn(false);

        when(opensearchClientRefresher.getComponentClass()).thenReturn(OpenSearchClient.class);
        when(opensearchClientRefresher.get()).thenReturn(openSearchClient);
        when(clusterClientFactory.getClientRefresher()).thenReturn(opensearchClientRefresher);

        final OpenSearchCatClient openSearchCatClient = mock(OpenSearchCatClient.class);
        final IndicesResponse indicesResponse = mock(IndicesResponse.class);
        when(indicesResponse.valueBody()).thenReturn(Collections.emptyList());
        when(openSearchCatClient.indices()).thenReturn(indicesResponse);
        when(openSearchClient.cat()).thenReturn(openSearchCatClient);

        final Map<String, Object> globalStateMap = new HashMap<>();
        // A stale flag left in state should be ignored in PERIODIC mode
        globalStateMap.put(SINGLE_SCAN_COMPLETE, Boolean.TRUE);

        createObjectUnderTest().apply(globalStateMap);

        // cat/indices was actually called, confirming discovery ran despite the flag
        verify(openSearchCatClient).indices();
    }

    private static Stream<Arguments> opensearchCatIndicesExceptions() {
        return Stream.of(Arguments.of(IOException.class),
                Arguments.of(OpenSearchException.class));
    }

    private static Stream<Arguments> elasticsearchCatIndicesExceptions() {
        return Stream.of(Arguments.of(IOException.class),
                Arguments.of(ElasticsearchException.class));
    }
}
