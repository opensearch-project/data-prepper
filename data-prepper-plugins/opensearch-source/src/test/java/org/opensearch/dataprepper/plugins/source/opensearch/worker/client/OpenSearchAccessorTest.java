/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.pit.CreatePitRequest;
import org.opensearch.client.opensearch.core.pit.CreatePitResponse;
import org.opensearch.client.opensearch.core.pit.DeletePitRecord;
import org.opensearch.client.opensearch.core.pit.DeletePitRequest;
import org.opensearch.client.opensearch.core.pit.DeletePitResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.client.opensearch.core.search.HitsMetadata;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.exceptions.SearchContextLimitException;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DeletePointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchContextType;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchPointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchPointInTimeResults;

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
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.OpenSearchAccessor.PIT_RESOURCE_LIMIT_ERROR_TYPE;

@ExtendWith(MockitoExtension.class)
public class OpenSearchAccessorTest {

    @Mock
    private OpenSearchClient openSearchClient;

    private SearchAccessor createObjectUnderTest() {
        return new OpenSearchAccessor(openSearchClient, SearchContextType.POINT_IN_TIME);
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

        assertThrows(SearchContextLimitException.class, () -> createObjectUnderTest().createPit(createPointInTimeRequest));
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

        assertThrows(OpenSearchException.class, () -> createObjectUnderTest().createPit(createPointInTimeRequest));
    }

    @Test
    void createPit_throws_runtime_exception_throws_IO_Exception() throws IOException {
        final String indexName = UUID.randomUUID().toString();
        final String keepAlive = UUID.randomUUID().toString();

        final CreatePointInTimeRequest createPointInTimeRequest = mock(CreatePointInTimeRequest.class);
        when(createPointInTimeRequest.getIndex()).thenReturn(indexName);
        when(createPointInTimeRequest.getKeepAlive()).thenReturn(keepAlive);

        when(openSearchClient.createPit(any(CreatePitRequest.class))).thenThrow(IOException.class);

        assertThrows(RuntimeException.class, () -> createObjectUnderTest().createPit(createPointInTimeRequest));
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

    @Test
    void delete_pit_does_not_throw_during_opensearch_exception() throws IOException {
        final String pitId = UUID.randomUUID().toString();

        final DeletePointInTimeRequest deletePointInTimeRequest = mock(DeletePointInTimeRequest.class);
        when(deletePointInTimeRequest.getPitId()).thenReturn(pitId);

        when(openSearchClient.deletePit(any(DeletePitRequest.class))).thenThrow(OpenSearchException.class);

        createObjectUnderTest().deletePit(deletePointInTimeRequest);
    }

    @Test
    void delete_pit_does_not_throw_exception_when_client_throws_IOException() throws IOException {
        final String pitId = UUID.randomUUID().toString();

        final DeletePointInTimeRequest deletePointInTimeRequest = mock(DeletePointInTimeRequest.class);
        when(deletePointInTimeRequest.getPitId()).thenReturn(pitId);

        when(openSearchClient.deletePit(any(DeletePitRequest.class))).thenThrow(IOException.class);

        createObjectUnderTest().deletePit(deletePointInTimeRequest);
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

        final SearchPointInTimeResults searchPointInTimeResults = createObjectUnderTest().searchWithPit(searchPointInTimeRequest);

        assertThat(searchPointInTimeResults, notNullValue());
        assertThat(searchPointInTimeResults.getDocuments(), notNullValue());
        assertThat(searchPointInTimeResults.getDocuments().size(), equalTo(2));

        assertThat(searchPointInTimeResults.getNextSearchAfter(), equalTo(secondHit.sort()));
    }
}
