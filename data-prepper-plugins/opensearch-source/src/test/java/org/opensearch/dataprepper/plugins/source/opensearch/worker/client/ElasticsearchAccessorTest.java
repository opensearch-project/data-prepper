/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.ClearScrollResponse;
import co.elastic.clients.elasticsearch.core.ClosePointInTimeRequest;
import co.elastic.clients.elasticsearch.core.ClosePointInTimeResponse;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeRequest;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeResponse;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.exceptions.SearchContextLimitException;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreateScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreateScrollResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DeletePointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DeleteScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.NoSearchContextSearchRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchContextType;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchPointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchScrollResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchWithSearchAfterResults;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.OpenSearchAccessor.PIT_RESOURCE_LIMIT_ERROR_TYPE;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.OpenSearchAccessor.SCROLL_RESOURCE_LIMIT_EXCEPTION_MESSAGE;

@ExtendWith(MockitoExtension.class)
public class ElasticsearchAccessorTest {

    @Mock
    private ElasticsearchClient elasticSearchClient;

    private SearchAccessor createObjectUnderTest() {
        return new ElasticsearchAccessor(elasticSearchClient, SearchContextType.POINT_IN_TIME);
    }

    @Test
    void create_pit_returns_expected_create_point_in_time_response() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final String keepAlive = UUID.randomUUID().toString();

        final CreatePointInTimeRequest createPointInTimeRequest = mock(CreatePointInTimeRequest.class);
        when(createPointInTimeRequest.getIndex()).thenReturn(indexName);
        when(createPointInTimeRequest.getKeepAlive()).thenReturn(keepAlive);

        final String pitId = UUID.randomUUID().toString();
        final OpenPointInTimeResponse createPitResponse = mock(OpenPointInTimeResponse.class);
        when(createPitResponse.id()).thenReturn(pitId);

        when(elasticSearchClient.openPointInTime(any(OpenPointInTimeRequest.class))).thenReturn(createPitResponse);

        final CreatePointInTimeResponse createPointInTimeResponse = createObjectUnderTest().createPit(createPointInTimeRequest);
        assertThat(createPointInTimeResponse, notNullValue());
        assertThat(createPointInTimeResponse.getPitCreationTime(), lessThanOrEqualTo(Instant.now().toEpochMilli()));
        assertThat(createPointInTimeResponse.getPitId(), equalTo(pitId));
    }

    @Test
    void create_scroll_returns_expected_create_scroll_response() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final String scrollTime = UUID.randomUUID().toString();
        final Integer size = new Random().nextInt(10);

        final CreateScrollRequest createScrollRequest = mock(CreateScrollRequest.class);
        when(createScrollRequest.getIndex()).thenReturn(indexName);
        when(createScrollRequest.getScrollTime()).thenReturn(scrollTime);
        when(createScrollRequest.getSize()).thenReturn(size);

        final String scrollId = UUID.randomUUID().toString();
        final SearchResponse<ObjectNode> searchResponse = mock(SearchResponse.class);
        when(searchResponse.scrollId()).thenReturn(scrollId);

        final HitsMetadata<ObjectNode> hitsMetadata = mock(HitsMetadata.class);
        final List<Hit<ObjectNode>> hits = new ArrayList<>();
        final Hit<ObjectNode> firstHit = mock(Hit.class);
        when(firstHit.id()).thenReturn(UUID.randomUUID().toString());
        when(firstHit.index()).thenReturn(UUID.randomUUID().toString());
        when(firstHit.source()).thenReturn(mock(ObjectNode.class));

        final Hit<ObjectNode> secondHit = mock(Hit.class);
        when(secondHit.id()).thenReturn(UUID.randomUUID().toString());
        when(secondHit.index()).thenReturn(UUID.randomUUID().toString());
        when(secondHit.source()).thenReturn(mock(ObjectNode.class));

        hits.add(firstHit);
        hits.add(secondHit);

        when(hitsMetadata.hits()).thenReturn(hits);
        when(searchResponse.hits()).thenReturn(hitsMetadata);

        final ArgumentCaptor<SearchRequest> searchRequestArgumentCaptor = ArgumentCaptor.forClass(SearchRequest.class);

        when(elasticSearchClient.search(searchRequestArgumentCaptor.capture(), eq(ObjectNode.class))).thenReturn(searchResponse);

        final CreateScrollResponse createScrollResponse = createObjectUnderTest().createScroll(createScrollRequest);
        assertThat(createScrollResponse, notNullValue());
        assertThat(createScrollResponse.getScrollId(), equalTo(scrollId));
        assertThat(createScrollResponse.getDocuments(), notNullValue());
        assertThat(createScrollResponse.getDocuments().size(), equalTo(2));
    }

    @Test
    void create_pit_with_exception_for_pit_limit_throws_SearchContextLimitException() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final String keepAlive = UUID.randomUUID().toString();

        final CreatePointInTimeRequest createPointInTimeRequest = mock(CreatePointInTimeRequest.class);
        when(createPointInTimeRequest.getIndex()).thenReturn(indexName);
        when(createPointInTimeRequest.getKeepAlive()).thenReturn(keepAlive);

        final ElasticsearchException elasticsearchException = mock(ElasticsearchException.class);
        final ErrorCause errorCause = mock(ErrorCause.class);
        final ErrorCause rootCause = mock(ErrorCause.class);
        when(rootCause.type()).thenReturn(PIT_RESOURCE_LIMIT_ERROR_TYPE);
        when(rootCause.reason()).thenReturn(UUID.randomUUID().toString());
        when(errorCause.causedBy()).thenReturn(rootCause);
        when(elasticsearchException.error()).thenReturn(errorCause);

        when(elasticSearchClient.openPointInTime(any(OpenPointInTimeRequest.class))).thenThrow(elasticsearchException);

        assertThrows(SearchContextLimitException.class, () -> createObjectUnderTest().createPit(createPointInTimeRequest));
    }

    @Test
    void create_scroll_with_exception_for_scroll_limit_throws_SearchContextLimitException() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final String scrollTime = UUID.randomUUID().toString();
        final Integer size = new Random().nextInt(10);

        final CreateScrollRequest createScrollRequest = mock(CreateScrollRequest.class);
        when(createScrollRequest.getIndex()).thenReturn(indexName);
        when(createScrollRequest.getScrollTime()).thenReturn(scrollTime);
        when(createScrollRequest.getSize()).thenReturn(size);

        final IOException exception = mock(IOException.class);
        when(exception.getMessage()).thenReturn(UUID.randomUUID() + SCROLL_RESOURCE_LIMIT_EXCEPTION_MESSAGE + UUID.randomUUID());

        when(elasticSearchClient.search(any(SearchRequest.class), eq(ObjectNode.class))).thenThrow(exception);

        assertThrows(SearchContextLimitException.class, () -> createObjectUnderTest().createScroll(createScrollRequest));
    }

    @Test
    void createPit_throws_Elasticsearch_exception_throws_that_exception() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final String keepAlive = UUID.randomUUID().toString();

        final CreatePointInTimeRequest createPointInTimeRequest = mock(CreatePointInTimeRequest.class);
        when(createPointInTimeRequest.getIndex()).thenReturn(indexName);
        when(createPointInTimeRequest.getKeepAlive()).thenReturn(keepAlive);

        final ElasticsearchException openSearchException = mock(ElasticsearchException.class);
        final ErrorCause errorCause = mock(ErrorCause.class);
        when(errorCause.causedBy()).thenReturn(null);
        when(openSearchException.error()).thenReturn(errorCause);

        when(elasticSearchClient.openPointInTime(any(OpenPointInTimeRequest.class))).thenThrow(openSearchException);

        assertThrows(ElasticsearchException.class, () -> createObjectUnderTest().createPit(createPointInTimeRequest));
    }

    @Test
    void createScroll_throws_Elasticsearch_exception_throws_that_exception() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final String scrollTime = UUID.randomUUID().toString();
        final Integer size = new Random().nextInt(10);

        final CreateScrollRequest createScrollRequest = mock(CreateScrollRequest.class);
        when(createScrollRequest.getIndex()).thenReturn(indexName);
        when(createScrollRequest.getScrollTime()).thenReturn(scrollTime);
        when(createScrollRequest.getSize()).thenReturn(size);

        final ElasticsearchException exception = mock(ElasticsearchException.class);

        when(elasticSearchClient.search(any(SearchRequest.class), eq(ObjectNode.class))).thenThrow(exception);

        assertThrows(ElasticsearchException.class, () -> createObjectUnderTest().createScroll(createScrollRequest));
    }

    @Test
    void createPit_throws_runtime_exception_throws_IO_Exception() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final String keepAlive = UUID.randomUUID().toString();

        final CreatePointInTimeRequest createPointInTimeRequest = mock(CreatePointInTimeRequest.class);
        when(createPointInTimeRequest.getIndex()).thenReturn(indexName);
        when(createPointInTimeRequest.getKeepAlive()).thenReturn(keepAlive);

        when(elasticSearchClient.openPointInTime(any(OpenPointInTimeRequest.class))).thenThrow(IOException.class);

        assertThrows(RuntimeException.class, () -> createObjectUnderTest().createPit(createPointInTimeRequest));
    }

    @Test
    void createScroll_throws_runtime_exception_when_throws_IO_Exception_that_is_not_search_context() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final String scrollTime = UUID.randomUUID().toString();
        final Integer size = new Random().nextInt(10);

        final CreateScrollRequest createScrollRequest = mock(CreateScrollRequest.class);
        when(createScrollRequest.getIndex()).thenReturn(indexName);
        when(createScrollRequest.getScrollTime()).thenReturn(scrollTime);
        when(createScrollRequest.getSize()).thenReturn(size);

        final IOException exception = mock(IOException.class);
        when(exception.getMessage()).thenReturn(UUID.randomUUID().toString());

        when(elasticSearchClient.search(any(SearchRequest.class), eq(ObjectNode.class))).thenThrow(exception);

        final RuntimeException exceptionThrown = assertThrows(RuntimeException.class, () -> createObjectUnderTest().createScroll(createScrollRequest));
        assertThat(exceptionThrown instanceof SearchContextLimitException, equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void delete_pit_with_no_exception_does_not_throw(final boolean successful) throws IOException {
        final String pitId = UUID.randomUUID().toString();

        final DeletePointInTimeRequest deletePointInTimeRequest = mock(DeletePointInTimeRequest.class);
        when(deletePointInTimeRequest.getPitId()).thenReturn(pitId);

        final ClosePointInTimeResponse deletePitResponse = mock(ClosePointInTimeResponse.class);
        when(deletePitResponse.succeeded()).thenReturn(successful);

        when(elasticSearchClient.closePointInTime(any(ClosePointInTimeRequest.class))).thenReturn(deletePitResponse);

        createObjectUnderTest().deletePit(deletePointInTimeRequest);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void delete_scroll_with_no_exception_does_not_throw(final boolean successful) throws IOException {
        final String scrollId = UUID.randomUUID().toString();

        final DeleteScrollRequest deleteScrollRequest = mock(DeleteScrollRequest.class);
        when(deleteScrollRequest.getScrollId()).thenReturn(scrollId);

        final ClearScrollResponse clearScrollResponse = mock(ClearScrollResponse.class);
        when(clearScrollResponse.succeeded()).thenReturn(successful);


        when(elasticSearchClient.clearScroll(any(ClearScrollRequest.class))).thenReturn(clearScrollResponse);

        createObjectUnderTest().deleteScroll(deleteScrollRequest);
    }

    @Test
    void delete_pit_does_not_throw_during_opensearch_exception() throws IOException {
        final String pitId = UUID.randomUUID().toString();

        final DeletePointInTimeRequest deletePointInTimeRequest = mock(DeletePointInTimeRequest.class);
        when(deletePointInTimeRequest.getPitId()).thenReturn(pitId);

        when(elasticSearchClient.closePointInTime(any(ClosePointInTimeRequest.class))).thenThrow(ElasticsearchException.class);

        createObjectUnderTest().deletePit(deletePointInTimeRequest);
    }

    @Test
    void delete_scroll_does_not_throw_during_elasticsearch_exception() throws IOException {
        final String scrollId = UUID.randomUUID().toString();

        final DeleteScrollRequest deleteScrollRequest = mock(DeleteScrollRequest.class);
        when(deleteScrollRequest.getScrollId()).thenReturn(scrollId);


        when(elasticSearchClient.clearScroll(any(ClearScrollRequest.class))).thenThrow(ElasticsearchException.class);

        createObjectUnderTest().deleteScroll(deleteScrollRequest);
    }

    @Test
    void delete_pit_does_not_throw_exception_when_client_throws_IOException() throws IOException {
        final String pitId = UUID.randomUUID().toString();

        final DeletePointInTimeRequest deletePointInTimeRequest = mock(DeletePointInTimeRequest.class);
        when(deletePointInTimeRequest.getPitId()).thenReturn(pitId);

        when(elasticSearchClient.closePointInTime(any(ClosePointInTimeRequest.class))).thenThrow(IOException.class);

        createObjectUnderTest().deletePit(deletePointInTimeRequest);
    }

    @Test
    void delete_scroll_does_not_throw_during_IO_exception() throws IOException {
        final String scrollId = UUID.randomUUID().toString();

        final DeleteScrollRequest deleteScrollRequest = mock(DeleteScrollRequest.class);
        when(deleteScrollRequest.getScrollId()).thenReturn(scrollId);


        when(elasticSearchClient.clearScroll(any(ClearScrollRequest.class))).thenThrow(IOException.class);

        createObjectUnderTest().deleteScroll(deleteScrollRequest);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void search_with_pit_returns_expected_SearchPointInTimeResponse(final boolean hasSearchAfter) throws IOException {
        final String pitId = UUID.randomUUID().toString();
        final Integer paginationSize = new Random().nextInt();
        final List<String> searchAfter = Collections.singletonList(UUID.randomUUID().toString());

        final SearchPointInTimeRequest searchPointInTimeRequest = mock(SearchPointInTimeRequest.class);
        when(searchPointInTimeRequest.getPitId()).thenReturn(pitId);
        when(searchPointInTimeRequest.getKeepAlive()).thenReturn("1m");
        when(searchPointInTimeRequest.getPaginationSize()).thenReturn(paginationSize);

        if (hasSearchAfter) {
            when(searchPointInTimeRequest.getSearchAfter()).thenReturn(searchAfter);
        } else {
            when(searchPointInTimeRequest.getSearchAfter()).thenReturn(null);
        }

        final SearchResponse<ObjectNode> searchResponse = mock(SearchResponse.class);
        final HitsMetadata<ObjectNode> hitsMetadata = mock(HitsMetadata.class);
        final List<Hit<ObjectNode>> hits = new ArrayList<>();
        final Hit<ObjectNode> firstHit = mock(Hit.class);
        when(firstHit.id()).thenReturn(UUID.randomUUID().toString());
        when(firstHit.index()).thenReturn(UUID.randomUUID().toString());
        when(firstHit.source()).thenReturn(mock(ObjectNode.class));

        final Hit<ObjectNode> secondHit = mock(Hit.class);
        when(secondHit.id()).thenReturn(UUID.randomUUID().toString());
        when(secondHit.index()).thenReturn(UUID.randomUUID().toString());
        when(secondHit.source()).thenReturn(mock(ObjectNode.class));
        when(secondHit.sort()).thenReturn(searchAfter);

        hits.add(firstHit);
        hits.add(secondHit);

        when(hitsMetadata.hits()).thenReturn(hits);
        when(searchResponse.hits()).thenReturn(hitsMetadata);

        final ArgumentCaptor<SearchRequest> searchRequestArgumentCaptor = ArgumentCaptor.forClass(SearchRequest.class);

        when(elasticSearchClient.search(searchRequestArgumentCaptor.capture(), eq(ObjectNode.class))).thenReturn(searchResponse);

        final SearchWithSearchAfterResults searchWithSearchAfterResults = createObjectUnderTest().searchWithPit(searchPointInTimeRequest);

        assertThat(searchWithSearchAfterResults, notNullValue());
        assertThat(searchWithSearchAfterResults.getDocuments(), notNullValue());
        assertThat(searchWithSearchAfterResults.getDocuments().size(), equalTo(2));

        assertThat(searchWithSearchAfterResults.getNextSearchAfter(), equalTo(secondHit.sort()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void search_without_search_context_returns_expected_SearchPointInTimeResponse(final boolean hasSearchAfter) throws IOException {
        final Integer paginationSize = new Random().nextInt();
        final String index = UUID.randomUUID().toString();
        final List<String> searchAfter = Collections.singletonList(UUID.randomUUID().toString());

        final NoSearchContextSearchRequest noSearchContextSearchRequest = mock(NoSearchContextSearchRequest.class);
        when(noSearchContextSearchRequest.getPaginationSize()).thenReturn(paginationSize);
        when(noSearchContextSearchRequest.getIndex()).thenReturn(index);

        if (hasSearchAfter) {
            when(noSearchContextSearchRequest.getSearchAfter()).thenReturn(searchAfter);
        } else {
            when(noSearchContextSearchRequest.getSearchAfter()).thenReturn(null);
        }

        final SearchResponse<ObjectNode> searchResponse = mock(SearchResponse.class);
        final HitsMetadata<ObjectNode> hitsMetadata = mock(HitsMetadata.class);
        final List<Hit<ObjectNode>> hits = new ArrayList<>();
        final Hit<ObjectNode> firstHit = mock(Hit.class);
        when(firstHit.id()).thenReturn(UUID.randomUUID().toString());
        when(firstHit.index()).thenReturn(index);
        when(firstHit.source()).thenReturn(mock(ObjectNode.class));

        final Hit<ObjectNode> secondHit = mock(Hit.class);
        when(secondHit.id()).thenReturn(UUID.randomUUID().toString());
        when(secondHit.index()).thenReturn(index);
        when(secondHit.source()).thenReturn(mock(ObjectNode.class));
        when(secondHit.sort()).thenReturn(searchAfter);

        hits.add(firstHit);
        hits.add(secondHit);

        when(hitsMetadata.hits()).thenReturn(hits);
        when(searchResponse.hits()).thenReturn(hitsMetadata);

        final ArgumentCaptor<SearchRequest> searchRequestArgumentCaptor = ArgumentCaptor.forClass(SearchRequest.class);

        when(elasticSearchClient.search(searchRequestArgumentCaptor.capture(), eq(ObjectNode.class))).thenReturn(searchResponse);

        final SearchWithSearchAfterResults searchWithSearchAfterResults = createObjectUnderTest().searchWithoutSearchContext(noSearchContextSearchRequest);

        assertThat(searchWithSearchAfterResults, notNullValue());
        assertThat(searchWithSearchAfterResults.getDocuments(), notNullValue());
        assertThat(searchWithSearchAfterResults.getDocuments().size(), equalTo(2));

        assertThat(searchWithSearchAfterResults.getNextSearchAfter(), equalTo(secondHit.sort()));
    }

    @Test
    void search_with_scroll_returns_expected_SearchScrollResponse() throws IOException {
        final String scrollId = UUID.randomUUID().toString();
        final String scrollTime = UUID.randomUUID().toString();

        final SearchScrollRequest searchScrollRequest = mock(SearchScrollRequest.class);
        when(searchScrollRequest.getScrollId()).thenReturn(scrollId);
        when(searchScrollRequest.getScrollTime()).thenReturn(scrollTime);

        final ScrollResponse<ObjectNode> searchResponse = mock(ScrollResponse.class);
        final HitsMetadata<ObjectNode> hitsMetadata = mock(HitsMetadata.class);
        final List<Hit<ObjectNode>> hits = new ArrayList<>();
        final Hit<ObjectNode> firstHit = mock(Hit.class);
        when(firstHit.id()).thenReturn(UUID.randomUUID().toString());
        when(firstHit.index()).thenReturn(UUID.randomUUID().toString());
        when(firstHit.source()).thenReturn(mock(ObjectNode.class));

        final Hit<ObjectNode> secondHit = mock(Hit.class);
        when(secondHit.id()).thenReturn(UUID.randomUUID().toString());
        when(secondHit.index()).thenReturn(UUID.randomUUID().toString());
        when(secondHit.source()).thenReturn(mock(ObjectNode.class));

        hits.add(firstHit);
        hits.add(secondHit);

        when(hitsMetadata.hits()).thenReturn(hits);
        when(searchResponse.hits()).thenReturn(hitsMetadata);
        when(searchResponse.scrollId()).thenReturn(scrollId);

        final ArgumentCaptor<ScrollRequest> searchRequestArgumentCaptor = ArgumentCaptor.forClass(ScrollRequest.class);

        when(elasticSearchClient.scroll(searchRequestArgumentCaptor.capture(), eq(ObjectNode.class))).thenReturn(searchResponse);

        final SearchScrollResponse searchScrollResponse = createObjectUnderTest().searchWithScroll(searchScrollRequest);

        assertThat(searchScrollResponse, notNullValue());
        assertThat(searchScrollResponse.getDocuments(), notNullValue());
        assertThat(searchScrollResponse.getDocuments().size(), equalTo(2));
        assertThat(searchScrollResponse.getScrollId(), equalTo(scrollId));
    }
}
