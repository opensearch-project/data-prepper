/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client;

import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePitRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePitResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreateScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreateScrollResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DeletePitRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DeleteScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchContextType;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchPitRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchPitResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchScrollResponse;

/**
 * Search Accessor can be used to perform searchs against search clusters.
 *
 * @since 2.4
 */
public interface SearchAccessor {
    /**
     * Information on whether how this SearchAccessor should be used by {@link org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchService}
     * @return the {@link SearchContextType} that has information on which search strategy should be used
     */
    SearchContextType getSearchContextType();

    /**
     * Creates a Point-In-Time (PIT) context for searching
     * @param createPitRequest the request for creating the PIT context
     * @return
     * @since 2.4
     */
    CreatePitResponse createPit(CreatePitRequest createPitRequest);

    /**
     * Searches using a PIT context
     * @param searchPitRequest payload for searching with PIT context
     * @return
     * @since 2.4
     */
    SearchPitResponse searchWithPit(SearchPitRequest searchPitRequest);

    /**
     * Deletes PITs
     * @param deletePitRequest contains the payload for deleting PIT contexts
     * @since 2.4
     */
    void deletePit(DeletePitRequest deletePitRequest);

    /**
     * Creates scroll context
     * @param createScrollRequest payload for creating the scroll context
     * @return
     * @since 2.4
     */
    CreateScrollResponse createScroll(CreateScrollRequest createScrollRequest);

    /**
     * Search with scroll context.
     * @param searchScrollRequest payload for searching with scroll context
     * @return
     */
    SearchScrollResponse searchWithScroll(SearchScrollRequest searchScrollRequest);

    /**
     * Deletes scroll contexts
     * @param deleteScrollRequest payload for deleting scroll contexts
     */
    void deleteScroll(DeleteScrollRequest deleteScrollRequest);
}
