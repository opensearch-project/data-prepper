/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.ScoreSort;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.Time;
import org.opensearch.client.opensearch._types.query_dsl.MatchAllQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.ClearScrollRequest;
import org.opensearch.client.opensearch.core.ClearScrollResponse;
import org.opensearch.client.opensearch.core.ScrollRequest;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.pit.CreatePitRequest;
import org.opensearch.client.opensearch.core.pit.CreatePitResponse;
import org.opensearch.client.opensearch.core.pit.DeletePitRequest;
import org.opensearch.client.opensearch.core.pit.DeletePitResponse;
import org.opensearch.client.opensearch.core.search.Pit;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.MetadataKeyAttributes.DOCUMENT_ID_METADATA_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.MetadataKeyAttributes.INDEX_METADATA_ATTRIBUTE_NAME;

public class OpenSearchAccessor implements SearchAccessor, ClusterClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchAccessor.class);

    static final String PIT_RESOURCE_LIMIT_ERROR_TYPE = "rejected_execution_exception";
    static final String SCROLL_RESOURCE_LIMIT_EXCEPTION_MESSAGE = "Trying to create too many scroll contexts";

    private final OpenSearchClient openSearchClient;
    private final SearchContextType searchContextType;

    public OpenSearchAccessor(final OpenSearchClient openSearchClient, final SearchContextType searchContextType) {
        this.openSearchClient = openSearchClient;
        this.searchContextType = searchContextType;
    }

    @Override
    public SearchContextType getSearchContextType() {
        return searchContextType;
    }

    @Override
    public CreatePointInTimeResponse createPit(final CreatePointInTimeRequest createPointInTimeRequest) {
        CreatePitResponse createPitResponse;
        try {
            createPitResponse = openSearchClient.createPit(CreatePitRequest.of(builder -> builder
                    .targetIndexes(createPointInTimeRequest.getIndex())
                    .keepAlive(new Time.Builder().time(createPointInTimeRequest.getKeepAlive()).build())));
        } catch (final OpenSearchException e) {
            if (isDueToPitLimitExceeded(e)) {
                throw new SearchContextLimitException(String.format("There was an error creating a new point in time for index '%s': %s", createPointInTimeRequest.getIndex(),
                        e.error().causedBy().reason()));
            }
            LOG.error("There was an error creating a point in time for OpenSearch: ", e);
            throw e;
        } catch (final IOException e) {
            LOG.error("There was an error creating a point in time for OpenSearch: ", e);
            throw new RuntimeException(e);
        }

        return CreatePointInTimeResponse.builder()
                .withPitId(createPitResponse.pitId())
                .withCreationTime(createPitResponse.creationTime())
                .build();
    }

    @Override
    public SearchWithSearchAfterResults searchWithPit(final SearchPointInTimeRequest searchPointInTimeRequest) {
        final SearchRequest searchRequest = SearchRequest.of(builder -> {
            builder
                    .pit(Pit.of(pitBuilder -> pitBuilder.id(searchPointInTimeRequest.getPitId()).keepAlive(searchPointInTimeRequest.getKeepAlive())))
                    .size(searchPointInTimeRequest.getPaginationSize())
                    .sort(SortOptions.of(sortOptionsBuilder -> sortOptionsBuilder.doc(ScoreSort.of(scoreSort -> scoreSort.order(SortOrder.Asc)))))
                    .query(Query.of(query -> query.matchAll(MatchAllQuery.of(matchAllQuery -> matchAllQuery.queryName("*")))));

            if (Objects.nonNull(searchPointInTimeRequest.getSearchAfter())) {
                builder.searchAfter(searchPointInTimeRequest.getSearchAfter());
            }

            return builder;
        });

        return searchWithSearchAfter(searchRequest);
    }

    @Override
    public void deletePit(final DeletePointInTimeRequest deletePointInTimeRequest) {
        try {
            final DeletePitResponse deletePitResponse = openSearchClient.deletePit(DeletePitRequest.of(builder -> builder.pitId(Collections.singletonList(deletePointInTimeRequest.getPitId()))));
            if (isPitDeletedSuccessfully(deletePitResponse)) {
                LOG.debug("Successfully deleted point in time id {}", deletePointInTimeRequest.getPitId());
            } else {
                LOG.warn("Point in time id {} was not deleted successfully. It will expire from keep-alive", deletePointInTimeRequest.getPitId());
            }
        } catch (final IOException | RuntimeException e) {
            LOG.error("There was an error deleting the point in time with id {} for OpenSearch. It will expire from keep-alive: ", deletePointInTimeRequest.getPitId(), e);
        }
    }

    @Override
    public CreateScrollResponse createScroll(final CreateScrollRequest createScrollRequest) {

        SearchResponse<ObjectNode> searchResponse;
        try {
            searchResponse = openSearchClient.search(SearchRequest.of(request -> request
                    .scroll(Time.of(time -> time.time(createScrollRequest.getScrollTime())))
                    .size(createScrollRequest.getSize())
                    .index(createScrollRequest.getIndex())), ObjectNode.class);
        } catch (final OpenSearchException e) {
            LOG.error("There was an error creating a scroll context for OpenSearch: ", e);
            throw e;
        } catch (final IOException e) {
            LOG.error("There was an error creating a scroll context for OpenSearch: ", e);
            if (isDueToScrollLimitExceeded(e)) {
                throw new SearchContextLimitException(String.format("There was an error creating a new scroll context for index '%s': %s",
                        createScrollRequest.getIndex(), e.getMessage()));
            }

            throw new RuntimeException(e);
        }

        return CreateScrollResponse.builder()
                .withCreationTime(Instant.now().toEpochMilli())
                .withScrollId(searchResponse.scrollId())
                .withDocuments(getDocumentsFromResponse(searchResponse))
                .build();
    }

    @Override
    public SearchScrollResponse searchWithScroll(final SearchScrollRequest searchScrollRequest) {
        SearchResponse<ObjectNode> searchResponse;
        try {
            searchResponse = openSearchClient.scroll(ScrollRequest.of(request -> request
                    .scrollId(searchScrollRequest.getScrollId())
                    .scroll(Time.of(time -> time.time(searchScrollRequest.getScrollTime())))), ObjectNode.class);
        } catch (final OpenSearchException e) {
            LOG.error("There was an error searching with a scroll context for OpenSearch: ", e);
            throw e;
        } catch (final IOException e) {
            LOG.error("There was an error searching with a scroll context for OpenSearch: ", e);
            throw new RuntimeException(e);
        }

        return SearchScrollResponse.builder()
                .withScrollId(searchResponse.scrollId())
                .withDocuments(getDocumentsFromResponse(searchResponse))
                .build();
    }

    @Override
    public void deleteScroll(final DeleteScrollRequest deleteScrollRequest) {
        try {
            final ClearScrollResponse clearScrollResponse = openSearchClient.clearScroll(ClearScrollRequest.of(request -> request.scrollId(deleteScrollRequest.getScrollId())));
            if (clearScrollResponse.succeeded()) {
                LOG.debug("Successfully deleted scroll context with id {}", deleteScrollRequest.getScrollId());
            } else {
                LOG.warn("Scroll context with id {} was not deleted successfully. It will expire from timing out on its own", deleteScrollRequest.getScrollId());
            }
        } catch (final IOException | RuntimeException e) {
            LOG.error("There was an error deleting the scroll context with id {} for OpenSearch. It will expire from timing out : ", deleteScrollRequest.getScrollId(), e);
        }
    }

    @Override
    public SearchWithSearchAfterResults searchWithoutSearchContext(final NoSearchContextSearchRequest noSearchContextSearchRequest) {
        final SearchRequest searchRequest = SearchRequest.of(builder -> {
            builder
                    .index(noSearchContextSearchRequest.getIndex())
                    .size(noSearchContextSearchRequest.getPaginationSize())
                    .sort(SortOptions.of(sortOptionsBuilder -> sortOptionsBuilder.doc(ScoreSort.of(scoreSort -> scoreSort.order(SortOrder.Asc)))))
                    .query(Query.of(query -> query.matchAll(MatchAllQuery.of(matchAllQuery -> matchAllQuery.queryName("*")))));

            if (Objects.nonNull(noSearchContextSearchRequest.getSearchAfter())) {
                builder.searchAfter(noSearchContextSearchRequest.getSearchAfter());
            }

            return builder;
        });

        return searchWithSearchAfter(searchRequest);
    }

    @Override
    public Object getClient() {
        return openSearchClient;
    }

    private boolean isPitDeletedSuccessfully(final DeletePitResponse deletePitResponse) {
        return Objects.nonNull(deletePitResponse.pits()) && deletePitResponse.pits().size() == 1
                && Objects.nonNull(deletePitResponse.pits().get(0)) && deletePitResponse.pits().get(0).successful();
    }

    private boolean isDueToPitLimitExceeded(final OpenSearchException e) {
        return Objects.nonNull(e.error()) && Objects.nonNull(e.error().causedBy()) && Objects.nonNull(e.error().causedBy().type())
                && PIT_RESOURCE_LIMIT_ERROR_TYPE.equals(e.error().causedBy().type());
    }

    private boolean isDueToScrollLimitExceeded(final IOException e) {
        return e.getMessage().contains(SCROLL_RESOURCE_LIMIT_EXCEPTION_MESSAGE);
    }

    private SearchWithSearchAfterResults searchWithSearchAfter(final SearchRequest searchRequest) {
        try {
            final SearchResponse<ObjectNode> searchResponse = openSearchClient.search(searchRequest, ObjectNode.class);

            final List<Event> documents = getDocumentsFromResponse(searchResponse);

            final List<String> nextSearchAfter = Objects.nonNull(searchResponse.hits().hits()) && !searchResponse.hits().hits().isEmpty() ?
                    searchResponse.hits().hits().get(searchResponse.hits().hits().size() - 1).sort() :
                    null;

            return SearchWithSearchAfterResults.builder()
                    .withDocuments(documents)
                    .withNextSearchAfter(nextSearchAfter)
                    .build();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Event> getDocumentsFromResponse(final SearchResponse<ObjectNode> searchResponse) {
        return searchResponse.hits().hits().stream()
                .map(hit -> JacksonEvent.builder()
                        .withData(hit.source())
                        .withEventMetadataAttributes(Map.of(DOCUMENT_ID_METADATA_ATTRIBUTE_NAME, hit.id(), INDEX_METADATA_ATTRIBUTE_NAME, hit.index()))
                        .withEventType(EventType.DOCUMENT.toString()).build())
                .collect(Collectors.toList());
    }
}
