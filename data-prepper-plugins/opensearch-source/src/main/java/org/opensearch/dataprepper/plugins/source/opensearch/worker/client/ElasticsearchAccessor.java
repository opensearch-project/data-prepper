/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.FieldSort;
import co.elastic.clients.elasticsearch._types.ScoreSort;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchAllQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.ClearScrollResponse;
import co.elastic.clients.elasticsearch.core.ClosePointInTimeRequest;
import co.elastic.clients.elasticsearch.core.ClosePointInTimeResponse;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeRequest;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeResponse;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.PointInTimeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginComponentRefresher;
import org.opensearch.dataprepper.plugins.source.opensearch.ElasticsearchClientRefresher;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.OpenSearchAccessor.SCROLL_RESOURCE_LIMIT_EXCEPTION_MESSAGE;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.MetadataKeyAttributes.DOCUMENT_ID_METADATA_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.MetadataKeyAttributes.INDEX_METADATA_ATTRIBUTE_NAME;

public class ElasticsearchAccessor implements SearchAccessor, ClusterClientFactory<ElasticsearchClient> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchAccessor.class);

    static final String PIT_RESOURCE_LIMIT_ERROR_TYPE = "rejected_execution_exception";
    static final String INDEX_NOT_FOUND_EXCEPTION = "index_not_found_exception";

    private final ElasticsearchClientRefresher elasticsearchClientRefresher;
    private final SearchContextType searchContextType;

    public ElasticsearchAccessor(final ElasticsearchClientRefresher elasticsearchClientRefresher, final SearchContextType searchContextType) {
        this.elasticsearchClientRefresher = elasticsearchClientRefresher;
        this.searchContextType = searchContextType;
    }

    @Override
    public SearchContextType getSearchContextType() {
        return searchContextType;
    }

    @Override
    public CreatePointInTimeResponse createPit(final CreatePointInTimeRequest createPointInTimeRequest) {

        OpenPointInTimeResponse openPointInTimeResponse;
        try {
            openPointInTimeResponse = elasticsearchClientRefresher.get()
                    .openPointInTime(OpenPointInTimeRequest.of(request -> request
                    .keepAlive(Time.of(time -> time.time(createPointInTimeRequest.getKeepAlive())))
                    .index(createPointInTimeRequest.getIndex())));
        } catch (final ElasticsearchException e) {
            if (isDueToPitLimitExceeded(e)) {
                throw new SearchContextLimitException(String.format("There was an error creating a new point in time for index '%s': %s", createPointInTimeRequest.getIndex(),
                        e.error().causedBy().reason()));
            }

            if (isDueToNoIndexFound(e)) {
                throw new IndexNotFoundException(String.format("The index '%s' could not be found and may have been deleted", createPointInTimeRequest.getIndex()));
            }
            LOG.error("There was an error creating a point in time for Elasticsearch: ", e);
            throw e;
        } catch (final IOException e) {
            LOG.error("There was an error creating a point in time for Elasticsearch: ", e);
            throw new RuntimeException(e);
        }

        return CreatePointInTimeResponse.builder()
                .withPitId(openPointInTimeResponse.id())
                .withCreationTime(Instant.now().toEpochMilli())
                .build();
    }

    @Override
    public SearchWithSearchAfterResults searchWithPit(final SearchPointInTimeRequest searchPointInTimeRequest) {
        final SearchRequest searchRequest = SearchRequest.of(builder ->  { builder
                .pit(PointInTimeReference.of(pit -> pit
                                .id(searchPointInTimeRequest.getPitId())
                                .keepAlive(Time.of(time -> time.time(searchPointInTimeRequest.getKeepAlive())))))
                .size(searchPointInTimeRequest.getPaginationSize())
                .sort(List.of(
                        SortOptions.of(sortOptionsBuilder -> sortOptionsBuilder.doc(ScoreSort.of(scoreSort -> scoreSort.order(SortOrder.Asc)))),
                        SortOptions.of(sortOptionsBuilder -> sortOptionsBuilder.field(FieldSort.of(fieldSortBuilder -> fieldSortBuilder.field("_id").order(SortOrder.Asc)))))
                )
                .query(Query.of(query -> query.matchAll(MatchAllQuery.of(matchAllQuery -> matchAllQuery))));

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
            final ClosePointInTimeResponse closePointInTimeResponse = elasticsearchClientRefresher.get()
                    .closePointInTime(ClosePointInTimeRequest.of(request -> request
                    .id(deletePointInTimeRequest.getPitId())));
            if (closePointInTimeResponse.succeeded()) {
                LOG.debug("Successfully deleted point in time id {}", deletePointInTimeRequest.getPitId());
            } else {
                LOG.warn("Point in time id {} was not deleted successfully. It will expire from keep-alive", deletePointInTimeRequest.getPitId());
            }
        } catch (final IOException | RuntimeException e) {
            LOG.error("There was an error deleting the point in time with id {} for Elasticsearch. It will expire from keep-alive: ", deletePointInTimeRequest.getPitId(), e);
        }
    }

    @Override
    public CreateScrollResponse createScroll(final CreateScrollRequest createScrollRequest) {

        SearchResponse<ObjectNode> searchResponse;

        try {
            searchResponse = elasticsearchClientRefresher.get()
                    .search(SearchRequest.of(request -> request
                    .scroll(Time.of(time -> time.time(createScrollRequest.getScrollTime())))
                    .sort(SortOptions.of(sortOptionsBuilder -> sortOptionsBuilder.doc(ScoreSort.of(scoreSort -> scoreSort.order(SortOrder.Asc)))))
                    .size(createScrollRequest.getSize())
                    .index(createScrollRequest.getIndex())), ObjectNode.class);
        } catch (final ElasticsearchException e) {
            if (isDueToNoIndexFound(e)) {
                throw new IndexNotFoundException(String.format("The index '%s' could not be found and may have been deleted", createScrollRequest.getIndex()));
            }

            LOG.error("There was an error creating a scroll context for Elasticsearch: ", e);
            throw e;
        } catch (final IOException e) {
            LOG.error("There was an error creating a scroll context for Elasticsearch: ", e);
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
            searchResponse = elasticsearchClientRefresher.get()
                    .scroll(ScrollRequest.of(request -> request
                    .scrollId(searchScrollRequest.getScrollId())
                    .scroll(Time.of(time -> time.time(searchScrollRequest.getScrollTime())))), ObjectNode.class);
        } catch (final ElasticsearchException e) {
            LOG.error("There was an error searching with a scroll context for Elasticsearch: ", e);
            throw e;
        } catch (final IOException e) {
            LOG.error("There was an error searching with a scroll context for Elasticsearch: ", e);
            throw new RuntimeException(e);
        }

        return SearchScrollResponse.builder()
                .withScrollId(searchResponse.scrollId())
                .withDocuments(getDocumentsFromResponse(searchResponse))
                .build();
    }

    @Override
    public void deleteScroll(DeleteScrollRequest deleteScrollRequest) {
        try {
            final ClearScrollResponse clearScrollResponse = elasticsearchClientRefresher.get()
                    .clearScroll(ClearScrollRequest.of(request -> request.scrollId(deleteScrollRequest.getScrollId())));
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
                    .sort(List.of(
                            SortOptions.of(sortOptionsBuilder -> sortOptionsBuilder.doc(ScoreSort.of(scoreSort -> scoreSort.order(SortOrder.Asc)))),
                            SortOptions.of(sortOptionsBuilder -> sortOptionsBuilder.field(FieldSort.of(fieldSortBuilder -> fieldSortBuilder.field("_id").order(SortOrder.Asc)))))
                    )
                    .query(Query.of(query -> query.matchAll(MatchAllQuery.of(matchAllQuery -> matchAllQuery))));

            if (Objects.nonNull(noSearchContextSearchRequest.getSearchAfter())) {
                builder.searchAfter(noSearchContextSearchRequest.getSearchAfter());
            }

            return builder;
        });

        return searchWithSearchAfter(searchRequest);
    }

    @Override
    public PluginComponentRefresher<ElasticsearchClient, OpenSearchSourceConfiguration> getClientRefresher() {
        return elasticsearchClientRefresher;
    }

    private SearchWithSearchAfterResults searchWithSearchAfter(final SearchRequest searchRequest) {

        try {
            final SearchResponse<ObjectNode> searchResponse = elasticsearchClientRefresher.get()
                    .search(searchRequest, ObjectNode.class);

            final List<Event> documents = getDocumentsFromResponse(searchResponse);

            final List<String> nextSearchAfter = Objects.nonNull(searchResponse.hits().hits()) && !searchResponse.hits().hits().isEmpty() ?
                    searchResponse.hits().hits().get(searchResponse.hits().hits().size() - 1).sort() :
                    null;

            return SearchWithSearchAfterResults.builder()
                    .withDocuments(documents)
                    .withNextSearchAfter(nextSearchAfter)
                    .build();
        } catch (final ElasticsearchException e) {
            if (isDueToNoIndexFound(e)) {
                throw new IndexNotFoundException(String.format("The index '%s' could not be found and may have been deleted", searchRequest.index()));
            }

            throw e;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isDueToPitLimitExceeded(final ElasticsearchException e) {
        return Objects.nonNull(e.error()) && Objects.nonNull(e.error().causedBy()) && Objects.nonNull(e.error().causedBy().type())
                && PIT_RESOURCE_LIMIT_ERROR_TYPE.equals(e.error().causedBy().type());
    }

    private boolean isDueToNoIndexFound(final ElasticsearchException e) {
        return Objects.nonNull(e.response()) && Objects.nonNull(e.response().error()) && Objects.nonNull(e.response().error().type())
                && INDEX_NOT_FOUND_EXCEPTION.equals(e.response().error().type());
    }

    private boolean isDueToScrollLimitExceeded(final IOException e) {
        return e.getMessage().contains(SCROLL_RESOURCE_LIMIT_EXCEPTION_MESSAGE);
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
