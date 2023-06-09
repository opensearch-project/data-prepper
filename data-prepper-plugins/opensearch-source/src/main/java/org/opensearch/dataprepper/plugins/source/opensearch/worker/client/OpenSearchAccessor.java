/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.Time;
import org.opensearch.client.opensearch.core.pit.CreatePitRequest;
import org.opensearch.client.opensearch.core.pit.CreatePitResponse;
import org.opensearch.client.opensearch.core.pit.DeletePitRequest;
import org.opensearch.client.opensearch.core.pit.DeletePitResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.exceptions.SearchContextLimitException;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreateScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreateScrollResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DeletePointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DeleteScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchContextType;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchPitRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchPitResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchScrollResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

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
    public SearchPitResponse searchWithPit(final SearchPitRequest searchPitRequest) {
        // todo: implement
        return null;
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
        } catch (final OpenSearchException e) {
            LOG.error("There was an error deleting the point in time with id {} for OpenSearch: ", deletePointInTimeRequest.getPitId(), e);
            throw e;
        } catch (IOException e) {
            LOG.error("There was an error deleting the point in time with id {} for OpenSearch: {}", deletePointInTimeRequest.getPitId(), e.getMessage());
            throw new RuntimeException(e);
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
