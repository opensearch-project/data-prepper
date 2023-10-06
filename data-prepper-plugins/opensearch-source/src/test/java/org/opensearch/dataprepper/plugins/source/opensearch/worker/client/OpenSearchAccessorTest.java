/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.ClearScrollRequest;
import org.opensearch.client.opensearch.core.ClearScrollResponse;
import org.opensearch.client.opensearch.core.ScrollRequest;
import org.opensearch.client.opensearch.core.ScrollResponse;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.pit.CreatePitRequest;
import org.opensearch.client.opensearch.core.pit.CreatePitResponse;
import org.opensearch.client.opensearch.core.pit.DeletePitRecord;
import org.opensearch.client.opensearch.core.pit.DeletePitRequest;
import org.opensearch.client.opensearch.core.pit.DeletePitResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.client.opensearch.core.search.HitsMetadata;
import org.opensearch.dataprepper.plugins.source.opensearch.ClientRefresher;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.exceptions.IndexNotFoundException;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.OpenSearchAccessor.INDEX_NOT_FOUND_EXCEPTION;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.OpenSearchAccessor.PIT_RESOURCE_LIMIT_ERROR_TYPE;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.OpenSearchAccessor.SCROLL_RESOURCE_LIMIT_EXCEPTION_MESSAGE;

@ExtendWith(MockitoExtension.class)
public class OpenSearchAccessorTest {

    @Mock
    private ClientRefresher clientRefresher;
    @Mock
    private OpenSearchClient openSearchClient;

    private SearchAccessor createObjectUnderTest() {
        return new OpenSearchAccessor(clientRefresher, SearchContextType.POINT_IN_TIME);
    }

    @BeforeEach
    void setup() {
        when(clientRefresher.get()).thenReturn(openSearchClient);
    }

    @Test
    void create_pit_returns_expected_create_point_in_time_response() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final String keepAlive = UUID.randomUUID().toString();

        final CreatePointInTimeRequest createPointInTimeRequest = mock(CreatePointInTimeRequest.class);
        when(createPointInTimeRequest.getIndex()).thenReturn(indexName);
        when(createPointInTimeRequest.getKeepAlive()).thenReturn(keepAlive);

        final String pitId = UUID.randomUUID().toString();
        final Long creationTime = new Random().nextLong();
        final CreatePitResponse createPitResponse = mock(CreatePitResponse.class);
        when(createPitResponse.creationTime()).thenReturn(creationTime);
        when(createPitResponse.pitId()).thenReturn(pitId);

        when(openSearchClient.createPit(any(CreatePitRequest.class))).thenReturn(createPitResponse);

        final CreatePointInTimeResponse createPointInTimeResponse = createObjectUnderTest().createPit(createPointInTimeRequest);
        assertThat(createPointInTimeResponse, notNullValue());
        assertThat(createPointInTimeResponse.getPitCreationTime(), equalTo(creationTime));
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

        when(openSearchClient.search(searchRequestArgumentCaptor.capture(), eq(ObjectNode.class))).thenReturn(searchResponse);

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

        final OpenSearchException openSearchException = mock(OpenSearchException.class);
        final ErrorCause errorCause = mock(ErrorCause.class);
        final ErrorCause rootCause = mock(ErrorCause.class);
        when(rootCause.type()).thenReturn(PIT_RESOURCE_LIMIT_ERROR_TYPE);
        when(rootCause.reason()).thenReturn(UUID.randomUUID().toString());
        when(errorCause.causedBy()).thenReturn(rootCause);
        when(openSearchException.error()).thenReturn(errorCause);

        when(openSearchClient.createPit(any(CreatePitRequest.class))).thenThrow(openSearchException);

        final SearchAccessor objectUnderTest = createObjectUnderTest();

        assertThrows(SearchContextLimitException.class, () -> objectUnderTest.createPit(createPointInTimeRequest));
    }

    @Test
    void create_pit_with_exception_for_index_not_found_throws_IndexNotFoundException() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final String keepAlive = UUID.randomUUID().toString();

        final CreatePointInTimeRequest createPointInTimeRequest = mock(CreatePointInTimeRequest.class);
        when(createPointInTimeRequest.getIndex()).thenReturn(indexName);
        when(createPointInTimeRequest.getKeepAlive()).thenReturn(keepAlive);

        final OpenSearchException openSearchException = mock(OpenSearchException.class);
        final ErrorResponse errorResponse = mock(ErrorResponse.class);
        final ErrorCause errorCause = mock(ErrorCause.class);
        when(errorCause.type()).thenReturn(INDEX_NOT_FOUND_EXCEPTION);
        when(errorResponse.error()).thenReturn(errorCause);
        when(openSearchException.response()).thenReturn(errorResponse);

        when(openSearchClient.createPit(any(CreatePitRequest.class))).thenThrow(openSearchException);

        final SearchAccessor objectUnderTest = createObjectUnderTest();

        assertThrows(IndexNotFoundException.class, () -> objectUnderTest.createPit(createPointInTimeRequest));
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

        when(openSearchClient.search(any(SearchRequest.class), eq(ObjectNode.class))).thenThrow(exception);

        final SearchAccessor objectUnderTest = createObjectUnderTest();

        assertThrows(SearchContextLimitException.class, () -> objectUnderTest.createScroll(createScrollRequest));
    }

    @Test
    void create_scroll_with_exception_for_index_not_found_throws_IndexNotFoundException() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final String scrollTime = UUID.randomUUID().toString();
        final Integer size = new Random().nextInt(10);

        final CreateScrollRequest createScrollRequest = mock(CreateScrollRequest.class);
        when(createScrollRequest.getIndex()).thenReturn(indexName);
        when(createScrollRequest.getScrollTime()).thenReturn(scrollTime);
        when(createScrollRequest.getSize()).thenReturn(size);

        final OpenSearchException openSearchException = mock(OpenSearchException.class);
        final ErrorResponse errorResponse = mock(ErrorResponse.class);
        final ErrorCause errorCause = mock(ErrorCause.class);
        when(errorCause.type()).thenReturn(INDEX_NOT_FOUND_EXCEPTION);
        when(errorResponse.error()).thenReturn(errorCause);
        when(openSearchException.response()).thenReturn(errorResponse);

        when(openSearchClient.search(any(SearchRequest.class), eq(ObjectNode.class))).thenThrow(openSearchException);

        final SearchAccessor objectUnderTest = createObjectUnderTest();

        assertThrows(IndexNotFoundException.class, () -> objectUnderTest.createScroll(createScrollRequest));
    }

    @Test
    void searchWithoutSearchContext_with_exception_for_index_not_found_throws_IndexNotFoundException() throws IOException {
        final Integer paginationSize = new Random().nextInt();
        final String index = UUID.randomUUID().toString();

        final NoSearchContextSearchRequest noSearchContextSearchRequest = mock(NoSearchContextSearchRequest.class);
        when(noSearchContextSearchRequest.getPaginationSize()).thenReturn(paginationSize);
        when(noSearchContextSearchRequest.getIndex()).thenReturn(index);

        final OpenSearchException openSearchException = mock(OpenSearchException.class);
        final ErrorResponse errorResponse = mock(ErrorResponse.class);
        final ErrorCause errorCause = mock(ErrorCause.class);
        when(errorCause.type()).thenReturn(INDEX_NOT_FOUND_EXCEPTION);
        when(errorResponse.error()).thenReturn(errorCause);
        when(openSearchException.response()).thenReturn(errorResponse);

        when(openSearchClient.search(any(SearchRequest.class), eq(ObjectNode.class))).thenThrow(openSearchException);

        final SearchAccessor objectUnderTest = createObjectUnderTest();

        assertThrows(IndexNotFoundException.class, () -> objectUnderTest.searchWithoutSearchContext(noSearchContextSearchRequest));
    }

    @Test
    void createPit_throws_OpenSearch_exception_throws_that_exception() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final String keepAlive = UUID.randomUUID().toString();

        final CreatePointInTimeRequest createPointInTimeRequest = mock(CreatePointInTimeRequest.class);
        when(createPointInTimeRequest.getIndex()).thenReturn(indexName);
        when(createPointInTimeRequest.getKeepAlive()).thenReturn(keepAlive);

        final OpenSearchException openSearchException = mock(OpenSearchException.class);
        final ErrorCause errorCause = mock(ErrorCause.class);
        when(errorCause.causedBy()).thenReturn(null);
        when(openSearchException.error()).thenReturn(errorCause);

        when(openSearchClient.createPit(any(CreatePitRequest.class))).thenThrow(openSearchException);

        final SearchAccessor objectUnderTest = createObjectUnderTest();

        assertThrows(OpenSearchException.class, () -> objectUnderTest.createPit(createPointInTimeRequest));
    }

    @Test
    void createScroll_throws_OpenSearch_exception_throws_that_exception() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final String scrollTime = UUID.randomUUID().toString();
        final Integer size = new Random().nextInt(10);

        final CreateScrollRequest createScrollRequest = mock(CreateScrollRequest.class);
        when(createScrollRequest.getIndex()).thenReturn(indexName);
        when(createScrollRequest.getScrollTime()).thenReturn(scrollTime);
        when(createScrollRequest.getSize()).thenReturn(size);

        final OpenSearchException exception = mock(OpenSearchException.class);

        when(openSearchClient.search(any(SearchRequest.class), eq(ObjectNode.class))).thenThrow(exception);

        final SearchAccessor objectUnderTest = createObjectUnderTest();

        assertThrows(OpenSearchException.class, () -> objectUnderTest.createScroll(createScrollRequest));
    }

    @Test
    void createPit_throws_runtime_exception_throws_IO_Exception() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final String keepAlive = UUID.randomUUID().toString();

        final CreatePointInTimeRequest createPointInTimeRequest = mock(CreatePointInTimeRequest.class);
        when(createPointInTimeRequest.getIndex()).thenReturn(indexName);
        when(createPointInTimeRequest.getKeepAlive()).thenReturn(keepAlive);

        when(openSearchClient.createPit(any(CreatePitRequest.class))).thenThrow(IOException.class);

        final SearchAccessor objectUnderTest = createObjectUnderTest();

        assertThrows(RuntimeException.class, () -> objectUnderTest.createPit(createPointInTimeRequest));
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

        when(openSearchClient.search(any(SearchRequest.class), eq(ObjectNode.class))).thenThrow(exception);

        final SearchAccessor objectUnderTest = createObjectUnderTest();

        final RuntimeException exceptionThrown = assertThrows(RuntimeException.class, () -> objectUnderTest.createScroll(createScrollRequest));
        assertThat(exceptionThrown instanceof SearchContextLimitException, equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void delete_pit_with_no_exception_does_not_throw(final boolean successful) throws IOException {
        final String pitId = UUID.randomUUID().toString();

        final DeletePointInTimeRequest deletePointInTimeRequest = mock(DeletePointInTimeRequest.class);
        when(deletePointInTimeRequest.getPitId()).thenReturn(pitId);

        final DeletePitResponse deletePitResponse = mock(DeletePitResponse.class);
        final DeletePitRecord deletePitRecord = mock(DeletePitRecord.class);
        when(deletePitRecord.successful()).thenReturn(successful);
        when(deletePitResponse.pits()).thenReturn(Collections.singletonList(deletePitRecord));

        when(openSearchClient.deletePit(any(DeletePitRequest.class))).thenReturn(deletePitResponse);

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


        when(openSearchClient.clearScroll(any(ClearScrollRequest.class))).thenReturn(clearScrollResponse);

        createObjectUnderTest().deleteScroll(deleteScrollRequest);
    }

    @Test
    void delete_pit_does_not_throw_during_opensearch_exception() throws IOException {
        final String pitId = UUID.randomUUID().toString();

        final DeletePointInTimeRequest deletePointInTimeRequest = mock(DeletePointInTimeRequest.class);
        when(deletePointInTimeRequest.getPitId()).thenReturn(pitId);

        when(openSearchClient.deletePit(any(DeletePitRequest.class))).thenThrow(OpenSearchException.class);

        createObjectUnderTest().deletePit(deletePointInTimeRequest);
    }

    @Test
    void delete_scroll_does_not_throw_during_opensearch_exception() throws IOException {
        final String scrollId = UUID.randomUUID().toString();

        final DeleteScrollRequest deleteScrollRequest = mock(DeleteScrollRequest.class);
        when(deleteScrollRequest.getScrollId()).thenReturn(scrollId);


        when(openSearchClient.clearScroll(any(ClearScrollRequest.class))).thenThrow(OpenSearchException.class);

        createObjectUnderTest().deleteScroll(deleteScrollRequest);
    }

    @Test
    void delete_pit_does_not_throw_exception_when_client_throws_IOException() throws IOException {
        final String pitId = UUID.randomUUID().toString();

        final DeletePointInTimeRequest deletePointInTimeRequest = mock(DeletePointInTimeRequest.class);
        when(deletePointInTimeRequest.getPitId()).thenReturn(pitId);

        when(openSearchClient.deletePit(any(DeletePitRequest.class))).thenThrow(IOException.class);

        createObjectUnderTest().deletePit(deletePointInTimeRequest);
    }

    @Test
    void delete_scroll_does_not_throw_during_IO_exception() throws IOException {
        final String scrollId = UUID.randomUUID().toString();

        final DeleteScrollRequest deleteScrollRequest = mock(DeleteScrollRequest.class);
        when(deleteScrollRequest.getScrollId()).thenReturn(scrollId);


        when(openSearchClient.clearScroll(any(ClearScrollRequest.class))).thenThrow(IOException.class);

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
        when(secondHit.sort()).thenReturn(Collections.singletonList(UUID.randomUUID().toString()));

        hits.add(firstHit);
        hits.add(secondHit);

        when(hitsMetadata.hits()).thenReturn(hits);
        when(searchResponse.hits()).thenReturn(hitsMetadata);

        final ArgumentCaptor<SearchRequest> searchRequestArgumentCaptor = ArgumentCaptor.forClass(SearchRequest.class);

        when(openSearchClient.search(searchRequestArgumentCaptor.capture(), eq(ObjectNode.class))).thenReturn(searchResponse);

        final SearchWithSearchAfterResults searchWithSearchAfterResults = createObjectUnderTest().searchWithPit(searchPointInTimeRequest);

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

        when(openSearchClient.scroll(searchRequestArgumentCaptor.capture(), eq(ObjectNode.class))).thenReturn(searchResponse);

        final SearchScrollResponse searchScrollResponse = createObjectUnderTest().searchWithScroll(searchScrollRequest);

        assertThat(searchScrollResponse, notNullValue());
        assertThat(searchScrollResponse.getDocuments(), notNullValue());
        assertThat(searchScrollResponse.getDocuments().size(), equalTo(2));
        assertThat(searchScrollResponse.getScrollId(), equalTo(scrollId));
    }
}
