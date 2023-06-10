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
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchContextType;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchPointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchPointInTimeResults;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchScrollResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    public SearchPointInTimeResults searchWithPit(final SearchPointInTimeRequest searchPointInTimeRequest) {
        try {
            final SearchResponse<ObjectNode> searchResponse = openSearchClient.search(
                    SearchRequest.of(builder -> {
                        builder
                            .pit(Pit.of(pitBuilder -> pitBuilder.id(searchPointInTimeRequest.getPitId()).keepAlive(searchPointInTimeRequest.getKeepAlive())))
                            .size(searchPointInTimeRequest.getPaginationSize())
                            .sort(SortOptions.of(sortOptionsBuilder -> sortOptionsBuilder.doc(ScoreSort.of(scoreSort -> scoreSort.order(SortOrder.Asc)))))
                            .query(Query.of(query -> query.matchAll(MatchAllQuery.of(matchAllQuery -> matchAllQuery.queryName("*")))));

                        if (Objects.nonNull(searchPointInTimeRequest.getSearchAfter())) {
                            builder.searchAfter(searchPointInTimeRequest.getSearchAfter());
                        }

                        return builder;
        }), ObjectNode.class);

            final List<Event> documents = searchResponse.hits().hits().stream()
                    .map(hit -> JacksonEvent.builder()
                            .withData(hit.source())
                            .withEventMetadataAttributes(Map.of(DOCUMENT_ID_METADATA_ATTRIBUTE_NAME, hit.id(), INDEX_METADATA_ATTRIBUTE_NAME, hit.index()))
                            .withEventType(EventType.DOCUMENT.toString()).build())
                    .collect(Collectors.toList());

            final List<String> nextSearchAfter = Objects.nonNull(searchResponse.hits().hits()) && !searchResponse.hits().hits().isEmpty() ?
                searchResponse.hits().hits().get(searchResponse.hits().hits().size() - 1).sort() :
                null;

            return SearchPointInTimeResults.builder()
                    .withDocuments(documents)
                    .withNextSearchAfter(nextSearchAfter)
                    .build();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
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
        //todo: implement
        return null;
    }

    @Override
    public SearchScrollResponse searchWithScroll(final SearchScrollRequest searchScrollRequest) {
        //todo: implement
        return null;
    }

    @Override
    public void deleteScroll(final DeleteScrollRequest deleteScrollRequest) {
        //todo: implement
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
}
